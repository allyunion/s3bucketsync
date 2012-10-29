#!/usr/bin/env python2.6
import boto.s3.connection
import boto.exception
from optparse import OptionParser
import sys
import os
import os.path
import ConfigParser

"""Purpose sync between buckets, similar to rsync.  This program is copyleft, under the GNU Public License."""

class S3BucketConf:
	def __init__(self, filename=None):
		self.ini = ConfigParser.SafeConfigParser()
		self.shared_access_key = None
		self.shared_secret_key = None
		self.src_s3url = None
		self.dest_s3url = None
		self.filename = filename

		if filename != None:
			if os.path.isfile(filename):
				f = file(filename, 'r')
				text = f.readline()
				f.close()
				# File is an s3cmd formatted configured file
				if '[default]' in text:
					self.parseS3cmdConfig(filename)
				else:
					self.parseConfig(filename)
		else:
			# Attempt to read AWS credentials from a boto configuration file
			boto_config = False
			for i in boto.BotoConfigLocations:
				if os.path.isfile(i) and not boto_config:
					boto_config = True
					parseBotoConfig(i)

			# Attempt to read AWS credentials from environmental variables
			if not boto_config:
				default_config_files = [os.path.join(os.getcwd(), '.s3synccfg'), os.path.join(os.environ['HOME'], '.s3synccfg')]
				if 'S3SYNCCONF' in os.environ:
					default_config_files.insert(0, os.environ['S3SYNCCONF'])

				config_test = False
				for filename in default_config_files:
					if os.path.isfile(filename) and not config_test:
						config_test = True
						self.parseConfig(filename)
				if not config_test:
					if 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
						self.shared_access_key = os.environ['AWS_ACCESS_KEY_ID']
						self.shared_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
					elif 'AWS_ACCESS_KEY' in os.environ and 'AWS_SECRET_KEY' in os.environ:
						self.shared_access_key = os.environ['AWS_ACCESS_KEY']
						self.shared_secret_key = os.environ['AWS_SECRET_KEY']

	def getIni(self, section, name, castf, fallback=None):
		try:
			return castf(self.ini.get(section, name))
		except Exception, e:
			print e
			return fallback

	def parseBotoConfig(self, filename):
		self.ini.read(filename)
		self.shared_access_key = self.getIni("Credentials", "aws_access_key_id", str)
		self.shared_secret_key = self.getIni("Credentials", "aws_secret_access_key", str)

	def parseS3cmdConfig(self, filename):
		self.ini.read(filename)
		self.shared_access_key = self.getIni("default", "access_key", str)
		self.shared_secret_key = self.getIni("default", "secret_key", str)

	def parseConfig(self, filename):
		self.ini.read(filename)
		self.shared_access_key = self.getIni("sync_default", "access_key", str)
		self.shared_secret_key = self.getIni("sync_default", "secret_key", str)
		self.src_s3url = self.getIni("sync_default", "source_s3_url", str)
		self.dest_s3url = self.getIni("sync_default", "destination_s3_url", str)

		if self.src_s3url != None and self.dest_s3url != None and self.src_s3url == self.dest_s3url:
			print "ERROR: Source s3 url and destination s3 url can not be the same!"
			sys.exit(2)
		if self.src_s3url != None and 's3://' != self.src_s3url[0:5]:
			print "ERROR: Invalid configuration option for source s3url!"
			sys.exit(3)
		if self.dest_s3url != None and 's3://' != self.dest_s3url[0:5]:
			print "ERROR: Invalid configuration option for destination s3url!"
			sys.exit(4)

	def isConfigured(self):
		if self.shared_access_key == None:
			return False
		elif self.shared_secret_key == None:
			return False
		else:
			return True

	def setSource_S3URL(self, s3url):
		if self.dest_s3url != None and self.dest_s3url == s3url:
			print "ERROR: Source s3 url and destination s3 url can not be the same!"
			sys.exit(2)
		if s3url == None:
			# FIXME: Create error class, and change below to raise the error instead.
			print "ERROR: No s3 URL specified when calling setSource_S3URL!"
			sys.exit(2)
		if 's3://' != s3url[0:5]:
			print "ERROR: invalid format for s3 url: %s" % s3url
			sys.exit(2)

		self.src_s3url = s3url

	def setDestination_S3URL(self, s3url):
		if self.src_s3url != None and self.src_s3url == s3url:
			print "ERROR: Source s3 url and destination s3 url can not be the same!"
			sys.exit(2)
		if s3url == None:
			# FIXME: Create error class, and change below to raise the error instead.
			print "ERROR: No s3 URL specified when calling setDestination_S3URL!"
			sys.exit(2)
		if 's3://' != s3url[0:5]:
			print "ERROR: invalid format for s3 url: %s" % s3url
			sys.exit(2)

		self.dest_s3url = s3url

	def getCredentials(self):
		return (self.shared_access_key, self.shared_secret_key)

	def getSource_S3URL(self):
		return self.src_s3url

	def getDestination_S3URL(self):
		return self.dest_s3url

	def writeConfigFile(self):
		f = file(self.filename, 'w')
		f.write('[sync_default]\n')
		f.write('access_key = %s\n' % self.shared_access_key)
		f.write('secret_key = %s\n' % self.shared_secret_key)
		if self.src_s3url != None:
			f.write('source_s3_url = %s\n' % self.src_s3url)
		if self.dest_s3url != None:
			f.write('destination_s3_url = %s\n' % self.dest_s3url)

	def testS3URL(self, s3url):
		try:
			print "Testing settings..."
			testBucket = boto.s3.connection.S3Connection(self.shared_access_key, self.shared_secret_key).get_bucket(s3url.replace('s3://', '', 1).split('/')[0])
			print "Success!"
			return True
		except boto.exception.S3ResponseError:
			print "Error!  Unable to get bucket [s3://%s] with provided credentials!  Try again..." % (s3url.replace('s3://', '', 1).split('/')[0])
			return False
		except KeyboardInterrupt:
			print "\nConfiguration changes not saved, exiting..."
			sys.exit(1)

	def configure(self):
		print "Enter new values or accept defaults in brackets with Enter."
		shared_access_key_text = "Enter shared AWS access key: "
		shared_secret_key_text = "Enter shared AWS secret key: "
		src_s3_url_text = "Enter source S3 URL: "
		dest_s3_url_text = "Enter destination S3 URL: "
 
		while 1:
			if self.isConfigured():
				looping = True
				while looping:
					try:
						val = raw_input("WARNING: credentials set for source and destination!  Continue? [y/N] ")
						if val.lower().startswith("y"):
							looping = False
						elif val.lower().startswith("n") or val == "":
							return None
						else:
							print "'%s' is an invalid choice.  Please enter 'y' or 'n'." % val
					except KeyboardInterrupt:
						print "\nConfiguration changes not saved, exiting..."
						sys.exit(1)

			file_loop = True
			while file_loop:
				try:
					filename = raw_input("Enter file location to save credentials [%s]: " % os.path.join(os.environ['HOME'], '.s3synccfg'))
					if filename == "":
						filename = os.path.join(os.environ['HOME'], '.s3synccfg')

					if os.path.isfile(filename):
						looping = True
						while looping:
							val = raw_input("WARNING: File [%s] exists!  Overwrite? [y/N] " % filename)
							if val.lower().startswith("y"):
								looping = False
							elif val.lower().startswith("n") or val == "":
								return None
							else:
								print "'%s' is an invalid choice.  Please enter 'y' or 'n'." % val
			
						try:
							f = file(filename, 'a')
							file_loop = False
							f.close()
						except IOError, e:
							print "Encountered error trying to write to file location: %s" % e

					else:
						try:
							f = file(filename, 'a')
							file_loop = False
							f.close()
							os.unlink(filename)
						except IOError, e:
							print "Encountered error trying to write to file location: %s" % e

				except KeyboardInterrupt:
					print "\nConfiguration changes not saved, exiting..."
					sys.exit(1)

			self.filename = filename
			looping = True
			src_looping = True
			dest_looping = True
			while looping:
				try:
					if self.shared_access_key != None:
						shared_access_key_text = "Enter shared AWS access key [%s]: " % self.shared_access_key

					if self.shared_secret_key != None:
						shared_secret_key_text = "Enter shared AWS secret key [%s]: " % self.shared_secret_key

					shared_access_key = raw_input(shared_access_key_text)
					if shared_access_key != '':
						self.shared_access_key = shared_access_key

					while self.shared_access_key == None or self.shared_access_key == '':
						if shared_access_key != '':
							self.shared_access_key = shared_access_key
						else:
							print "Source AWS access key can not be blank!"
							shared_access_key = raw_input(shared_access_key_text)

					shared_secret_key = raw_input(shared_secret_key_text)
					if shared_secret_key != '':
						self.shared_secret_key = shared_secret_key

					while self.shared_secret_key == None or self.shared_secret_key == '':
						if shared_secret_key != '':
							self.src_sectet_key = shared_secret_key
						else:
							print "Source AWS secret key can not be blank!"
							shared_secret_key = raw_input(shared_secret_key_text)

					conn = boto.s3.connection.S3Connection(self.shared_access_key, self.shared_secret_key)
					while src_looping:
						if self.src_s3url != None:
							src_s3_url_text = "Enter source S3 URL [%s]: " % self.src_s3url
						src_s3url = raw_input(src_s3_url_text)

						if src_s3url != '' and 's3://' == src_s3url[0:5]:
							self.src_s3url = src_s3url
							if self.testS3URL(self.src_s3url):
								src_looping = False
						elif src_s3url == '' and self.src_s3url != None and self.src_s3url != '':
							if self.testS3URL(self.src_s3url):
								src_looping = False
						else:
							print "[%s] is a malformed s3 URL!  Try again..." % src_s3url

					while dest_looping:
						if self.dest_s3url != None:
							dest_s3_url_text = "Enter destination S3 URL [%s]: " % self.dest_s3url
						dest_s3url = raw_input(dest_s3_url_text)

						if dest_s3url != '' and 's3://' == dest_s3url[0:5]:
							self.dest_s3url = dest_s3url
							if self.testS3URL(self.dest_s3url):
								dest_looping = False
						elif dest_s3url == '' and self.dest_s3url != None and self.dest_s3url != '':
							if self.testS3URL(self.dest_s3url):
								dest_looping = False
						else:
							print "[%s] is a malformed s3 URL!  Try again..." % dest_s3url

					if src_looping == False and dest_looping == False:
						looping = False

				except boto.exception.NoAuthHandlerFound:
					print "Invalid credentials!  Try again..."
				except KeyboardInterrupt:
					print "\nConfiguration changes not saved, exiting..."
					sys.exit(1)

			val = raw_input("Save settings? [Y/n] ")
			if val.lower().startswith("y") or val == "":
				self.writeConfigFile()
				break
			val = raw_input("Retry configuration? [Y/n] ")
			if val.lower().startswith("n"):
				print "No settings saved, exiting..."
				sys.exit(1)

class S3BucketSync:
	def __init__(self, s3conf, forcesync=False, forcecopy=False, verbose=False, debug=False):
		self.config = {}
		creds = s3conf.getCredentials()
		self.config['shared_access_key'] = creds[0]
		self.config['shared_secret_key'] = creds[1]
		del creds

		self.config['src_s3_url'] = s3conf.getSource_S3URL()
		self.config['src_s3_bucket'] = self.config['src_s3_url'].replace('s3://', '', 1).split('/')[0]
		self.config['src_s3_path'] = self.config['src_s3_url'].replace('s3://' + self.config['src_s3_bucket'], '', 1)
		if self.config['src_s3_path'][0] == '/' and len(self.config['src_s3_path']) >= 1:
			self.config['src_s3_path'] = self.config['src_s3_path'][1:]
		self.config['dest_s3_url'] = s3conf.getDestination_S3URL()
		self.config['dest_s3_bucket'] = self.config['dest_s3_url'].replace('s3://', '', 1).split('/')[0]
		self.config['dest_s3_path'] = self.config['dest_s3_url'].replace('s3://' + self.config['dest_s3_bucket'], '', 1)
		if self.config['dest_s3_path'][0] == '/' and len(self.config['dest_s3_path']) >= 1:
			self.config['dest_s3_path'] = self.config['dest_s3_path'][1:]

		self.verbose = verbose
		self.debug = debug
		self.forcesync = forcesync
		self.forcecopy = forcecopy

		self.conn = boto.s3.connection.S3Connection(self.config['shared_access_key'], self.config['shared_secret_key'])

		self.src_bucket = self.conn.get_bucket(self.config['src_s3_bucket'])
		self.src_filelist = self.src_bucket.list(self.config['src_s3_path'])
		count = 0
		for i in self.src_filelist:
			count += 1
		if count == 1:
			if self.config['src_s3_path'][-1] != '/':
				self.config['src_s3_path'] += '/'
				self.config['src_s3_url'] += '/'
				s3conf.setSource_S3URL(self.config['src_s3_url'])
				s3conf.writeConfigFile()

		self.dest_bucket = self.conn.get_bucket(self.config['dest_s3_bucket'])
		self.dest_filelist = self.dest_bucket.list(self.config['dest_s3_path'])
		count = 0
		for i in self.dest_filelist:
			count += 1
		if count == 1:
			if self.config['dest_s3_path'][-1] != '/':
				self.config['dest_s3_path'] += '/'
				self.config['dest_s3_url'] += '/'
				s3conf.setDestination_S3URL(self.config['dest_s3_url'])
				s3conf.writeConfigFile()


	def sync(self):
		if self.config['src_s3_path'] == self.config['dest_s3_path']:
			if self.forcecopy:
				for key in self.src_filelist:
					destKey = self.dest_bucket.get_key(key.name)
					if self.verbose:
						print "Force copying s3://%s/%s to s3://%s/%s" % (self.config['src_s3_bucket'], key.name, self.config['dest_s3_bucket'], key.name)
					key.copy(self.config['dest_s3_bucket'], key.name)
			else:
				for key in self.src_filelist:
					destKey = self.dest_bucket.get_key(key.name)
					if not destKey or destKey.size != key.size:
						if self.verbose:
							print "Copying s3://%s/%s to s3://%s/%s" % (self.config['src_s3_bucket'], key.name, self.config['dest_s3_bucket'], key.name)
						key.copy(self.config['dest_s3_bucket'], key.name)

			if self.forcesync:
				self.dest_filelist = self.dest_bucket.list(self.config['dest_s3_path'])
				for key in self.dest_filelist:
					srcKey = self.src_bucket.get_key(key.name)
					if not srcKey:
						key.delete()
									
		else:
			if self.forcecopy:
				for key in self.src_filelist: 
					destKeyName = self.config['dest_s3_path'] + key.name.replace(self.config['src_s3_path'], '', 1)
					destKey = self.dest_bucket.get_key(destKeyName)
					if self.verbose:
						print "Force copying s3://%s/%s to s3://%s/%s"  % (self.config['src_s3_bucket'], key.name, self.config['dest_s3_bucket'], destKeyName)
					key.copy(self.config['dest_s3_bucket'], destKeyName)
			else:
				for key in self.src_filelist: 
					destKeyName = self.config['dest_s3_path'] + key.name.replace(self.config['src_s3_path'], '', 1)
					destKey = self.dest_bucket.get_key(destKeyName)
					if not destKey or destKey.size != key.size:
						if self.verbose:
							print "Copying s3://%s/%s to s3://%s/%s"  % (self.config['src_s3_bucket'], key.name, self.config['dest_s3_bucket'], destKeyName)
						key.copy(self.config['dest_s3_bucket'], destKeyName)
			

			if self.forcesync:
				self.dest_filelist = self.dest_bucket.list(self.config['dest_s3_path'])
				for key in self.dest_filelist:
					srcKeyName = self.config['src_s3_path'] + key.name.replace(self.config['dest_s3_path'], '', 1)
					srcKey = self.src_bucket.get_key(srcKeyName)
					if not srcKey:
						if self.verbose:
							print "Removing s3://%s/%s from destination bucket..." % (self.config['dest_s3_bucket'], key.name)
						key.delete()
					

if __name__ == "__main__":
	parser = OptionParser(usage="%prog [-c] [-f] [-v] [-d] SOURCE_S3_URL DEST_S3_URL", version="%prog 0.1")
	parser.set_defaults(verbose=False, forcesync=False, configure=False, configfile=None, forcerun=False, debug=False, forcecopy=False)
	parser.add_option("-C", "--configure", action="store_true", dest="configure", help="Invoke interactive (re)configuration tool.  All other options are ignored in this mode.")
	parser.add_option("-c", "--config", dest="configfile", help="Config file name. Defaults to [current_working_directory]/.s3synccfg and attempts %s next" % os.path.join(os.environ['HOME'], '.s3synccfg'))
	parser.add_option("-r", "--run", action="store_true", dest="forcerun", help="Run a sync using the defaults from the default configuration file locations")
	parser.add_option("-s", "--sync", action="store_true", dest="forcesync", help="Force sync (delete extra files) on destination bucket")
	parser.add_option("-f", "--force", action="store_true", dest="forcecopy", help="Force copy operation, regardless if the file exists or not")
	parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="Verbose file copy operations")
	parser.add_option("-d", "--debug", action="store_true", dest="debug", help="Debugging mode")
	(options, args) = parser.parse_args()

	if len(sys.argv) == 1:
		parser.print_help()
		sys.exit(1)

	s3conf = S3BucketConf(options.configfile)

	if options.configure:
		s3conf.configure()
	elif s3conf.isConfigured():
		if options.forcerun:
			s3 = S3BucketSync(s3conf, options.forcesync, options.verbose)
			s3.sync()
			sys.exit(0)
		elif options.configfile != None and s3conf.getSource_S3URL() != None and s3conf.getDestination_S3URL() != None:
			s3 = S3BucketSync(s3conf, options.forcesync, options.verbose)
			s3.sync()
			sys.exit(0)
		elif len(args) == 0:
			parser.print_help()
			sys.exit(1)
		elif len(args) == 1:
			print "ERROR: A DEST_S3_URL is required!"
			parser.print_help()
			sys.exit(1)
		elif len(args) != 2:
			print "ERROR: SOURCE_S3_URL and DEST_S3_URL are required!"
			parser.print_help()
			sys.exit(1)

		usage_exit = False
		for i in args:
			if 's3://' != i[0:5]:
				print "ERROR: '%s' is not a valid s3 URL!" % i
				usage_exit = True

		if usage_exit:
			parser.print_help()
			sys.exit(1)

		if args[0] == args[1]:
			print "ERROR: SOURCE and DESTINATION are the same!"
			sys.exit(1)

		s3conf.setSource_S3URL(args[0])
		s3conf.setDestination_S3URL(args[1])

		s3 = S3BucketSync(s3conf, options.forcesync, options.verbose)
		s3.sync()
		sys.exit(0)
	else:
		print "ERROR: No configuration files found!  Please use the -C option to setup a configuration file."
		parser.print_help()
		sys.exit(1)
