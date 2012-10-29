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
		self.src_access_key = None
		self.src_secret_key = None
		self.src_s3url = None
		self.dest_access_key = None
		self.dest_secret_key = None
		self.dest_s3url = None

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
						self.src_access_key = os.environ['AWS_ACCESS_KEY_ID']
						self.src_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
						self.dest_access_key = os.environ['AWS_ACCESS_KEY_ID']
						self.dest_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
					elif 'AWS_ACCESS_KEY' in os.environ and 'AWS_SECRET_KEY' in os.environ:
						self.src_access_key = os.environ['AWS_ACCESS_KEY']
						self.src_secret_key = os.environ['AWS_SECRET_KEY']
						self.dest_access_key = os.environ['AWS_ACCESS_KEY']
						self.dest_secret_key = os.environ['AWS_SECRET_KEY']

	def getIni(self, section, name, castf, fallback=None):
		try:
			return castf(self.ini.get(section, name))
		except Exception, e:
			print e
			return fallback

	def parseBotoConfig(self, filename):
		self.ini.read(filename)
		self.src_access_key = self.getIni("Credentials", "aws_access_key_id", str)
		self.src_secret_key = self.getIni("Credentials", "aws_secret_access_key", str)
		self.dest_access_key = self.getIni("Credentials", "aws_access_key_id", str)
		self.dest_secret_key = self.getIni("Credentials", "aws_secret_access_key", str)

	def parseS3cmdConfig(self, filename):
		self.ini.read(filename)
		self.src_access_key = self.getIni("default", "access_key", str)
		self.src_secret_key = self.getIni("default", "secret_key", str)
		self.dest_access_key = self.getIni("default", "access_key", str)
		self.dest_secret_key = self.getIni("default", "secret_key", str)

	def parseConfig(self, filename):
		self.ini.read(filename)
		self.src_access_key = self.getIni("source", "access_key", str)
		self.src_secret_key = self.getIni("source", "secret_key", str)
		self.src_s3url = self.getIni("source", "s3url", str)
		self.dest_access_key = self.getIni("destination", "access_key", str)
		self.dest_secret_key = self.getIni("destination", "secret_key", str)
		self.dest_s3url = self.getIni("destination", "s3url", str)

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
		if self.src_access_key == None:
			return False
		elif self.src_secret_key == None:
			return False
		elif self.dest_access_key == None:
			return False
		elif self.dest_access_key == None:
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

	def getSource_Credentials(self):
		return (self.src_access_key, self.src_secret_key)

	def getSource_S3URL(self):
		return self.src_s3url

	def getDestination_Credentials(self):
		return (self.dest_access_key, self.dest_secret_key)

	def getDestination_S3URL(self):
		return self.dest_s3url

	def writeConfigFile(self, filename):
		f = file(filename, 'w')
		f.write('[source]\n')
		f.write('access_key = %s\n' % self.src_access_key)
		f.write('secret_key = %s\n' % self.src_secret_key)
		if self.src_s3url != None:
			f.write('s3url = %s\n' % self.src_s3url)
		f.write('[destination]\n')
		f.write('access_key = %s\n' % self.dest_access_key)
		f.write('secret_key = %s\n' % self.dest_secret_key)
		if self.dest_s3url != None:
			f.write('s3url = %s\n' % self.dest_s3url)

	def configure(self):
		print "Enter new values or accept defaults in brackets with Enter."
		src_access_key_text = "Enter source AWS access key: "
		src_secret_key_text = "Enter source AWS secret key: "
		src_s3_url_text = "Enter source S3 URL: "
		dest_access_key_text = "Enter destination AWS access key: "
		dest_secret_key_text = "Enter destination AWS secret key: "
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

			looping = True
			while looping:
				try:
					if self.src_access_key != None:
						src_access_key_text = "Enter source AWS access key [%s]: " % self.src_access_key

					if self.src_secret_key != None:
						src_secret_key_text = "Enter source AWS secret key [%s]: " % self.src_secret_key

					src_access_key = raw_input(src_access_key_text)
					if src_access_key != '':
						self.src_access_key = src_access_key

					while self.src_access_key == None or self.src_access_key == '':
						if src_access_key != '':
							self.src_access_key = src_access_key
						else:
							print "Source AWS access key can not be blank!"
							src_access_key = raw_input(src_access_key_text)

					src_secret_key = raw_input(src_secret_key_text)
					if src_secret_key != '':
						self.src_secret_key = src_secret_key

					while self.src_secret_key == None or self.src_secret_key == '':
						if src_secret_key != '':
							self.src_sectet_key = src_secret_key
						else:
							print "Source AWS secret key can not be blank!"
							src_secret_key = raw_input(src_secret_key_text)

					conn = boto.s3.connection.S3Connection(self.src_access_key, self.src_secret_key)
					inner_looping = True
					while inner_looping:
						if self.src_s3url != None:
							src_s3_url_text = "Enter source S3 URL [%s]: " % self.src_s3url
						src_s3url = raw_input(src_s3_url_text)

						if src_s3url != '' and 's3://' == src_s3url[0:5]:
							self.src_s3url = src_s3url
							break
						elif src_s3url == '' and self.src_s3url != None and self.src_s3url != '':
							break
						else:
							print "[%s] is a malformed s3 URL!  Try again..." % src_s3url
					try:
						print "Testing settings..."
						srcBucket = boto.s3.connection.S3Connection(self.src_access_key, self.src_secret_key).get_bucket(self.src_s3url.replace('s3://', '', 1).split('/')[0])
						looping = False
						print "Success!"
					except boto.exception.S3ResponseError:
						print "Error!  Unable to get bucket [s3://%s] with provided credentials!  Try again..." % (self.src_s3url.replace('s3://', '', 1).split('/')[0])
				except boto.exception.NoAuthHandlerFound:
					print "Invalid credentials!  Try again..."
				except KeyboardInterrupt:
					print "\nConfiguration changes not saved, exiting..."
					sys.exit(1)

			looping = True
			while looping:
				try:
					if self.dest_access_key != None:
						dest_access_key_text = "Enter source AWS access key [%s]: " % self.dest_access_key

					if self.dest_secret_key != None:
						dest_secret_key_text = "Enter source AWS secret key [%s]: " % self.dest_secret_key

					dest_access_key = raw_input(dest_access_key_text)
					if dest_access_key != '':
						self.dest_access_key = dest_access_key

					while self.dest_access_key == None or self.dest_access_key == '':
						if dest_access_key != '':
							self.dest_access_key = dest_access_key
						else:
							print "Destination AWS access key can not be blank!"
							dest_access_key = raw_input(dest_access_key_text)

					dest_secret_key = raw_input(dest_secret_key_text)
					if dest_secret_key != '':
						self.dest_secret_key = dest_secret_key

					while self.dest_secret_key == None or self.dest_secret_key == '':
						if dest_secret_key != '':
							self.dest_sectet_key = dest_secret_key
						else:
							print "Destination AWS secret key can not be blank!"
							dest_secret_key = raw_input(dest_secret_key_text)

					conn = boto.s3.connection.S3Connection(self.dest_access_key, self.dest_secret_key)
					inner_looping = True
					while inner_looping:
						if self.dest_s3url != None:
							dest_s3_url_text = "Enter source S3 URL [%s]: " % self.dest_s3url
						dest_s3url = raw_input(dest_s3_url_text)

						if dest_s3url != '' and 's3://' == dest_s3url[0:5]:
							self.dest_s3url = dest_s3url
							break
						elif dest_s3url == '' and self.dest_s3url != None and self.dest_s3url != '':
							break
						else:
							print "[%s] is a malformed s3 URL!  Try again..." % dest_s3url
					try:
						print "Testing settings..."
						destBucket = boto.s3.connection.S3Connection(self.dest_access_key, self.dest_secret_key).get_bucket(self.dest_s3url.replace('s3://', '', 1).split('/')[0])
						looping = False
						print "Success!"
					except boto.exception.S3ResponseError:
						print "Error!  Unable to get bucket [s3://%s] with provided credentials!  Try again..." % (self.dest_s3url.replace('s3://', '', 1).split('/')[0])
				except boto.exception.NoAuthHandlerFound:
					print "Invalid credentials!  Try again..."
				except KeyboardInterrupt:
					print "\nConfiguration changes not saved, exiting..."
					sys.exit(1)

			val = raw_input("Save settings? [Y/n] ")
			if val.lower().startswith("y") or val == "":
				self.writeConfigFile(filename)
				break
			val = raw_input("Retry configuration? [Y/n] ")
			if val.lower().startswith("n"):
				print "No settings saved, exiting..."
				sys.exit(1)

class S3BucketSync:
	def __init__(self, s3conf, forcedelete, verbose=False):
		self.config = {}
		creds = s3conf.getSource_Credentials()
		self.config['src_access_key'] = creds[0]
		self.config['src_secret_key'] = creds[1]
		self.config['src_s3_url'] = s3conf.getSource_S3URL()
		self.config['src_s3_bucket'] = self.config['src_s3_url'].replace('s3://', '', 1).split('/')[0]
		self.config['src_s3_path'] = self.config['src_s3_url'].replace('s3://' + self.config['src_s3_bucket'], '', 1)
		if self.config['src_s3_path'][0] == '/' and len(self.config['src_s3_path']) >= 1:
			self.config['src_s3_path'] = self.config['src_s3_path'][1:]
		creds = s3conf.getDestination_Credentials()
		self.config['dest_access_key'] = creds[0]
		self.config['dest_secret_key'] = creds[1]
		self.config['dest_s3_url'] = s3conf.getDestination_S3URL()
		self.config['dest_s3_bucket'] = self.config['dest_s3_url'].replace('s3://', '', 1).split('/')[0]
		self.config['dest_s3_path'] = self.config['dest_s3_url'].replace('s3://' + self.config['dest_s3_bucket'], '', 1)
		if self.config['dest_s3_path'][0] == '/' and len(self.config['dest_s3_path']) >= 1:
			self.config['dest_s3_path'] = self.config['dest_s3_path'][1:]
		del creds

		self.verbose = verbose
		self.forcedelete = forcedelete

		self.src_conn = boto.s3.connection.S3Connection(self.config['src_access_key'], self.config['src_secret_key'])
		self.dest_conn = boto.s3.connection.S3Connection(self.config['dest_access_key'], self.config['dest_access_key'])

	def sync(self):
		src_bucket = self.src_conn.get_bucket(self.config['src_s3_bucket'])
		dest_bucket = self.dest_conn.get_bucket(self.config['dest_s3_bucket'])
		for key in self.src_bucket.list(self.config['src_s3_path']):
			print key.name


if __name__ == "__main__":
	parser = OptionParser(usage="%prog [-c] [-f] [-v] [-d] SOURCE_S3_URL DEST_S3_URL", version="%prog 0.1")
	parser.set_defaults(verbose=False, forcedelete=False, configure=False, configfile=None, forcerun=False)
	parser.add_option("-C", "--configure", action="store_true", dest="configure", help="Invoke interactive (re)configuration tool.  All other options are ignored in this mode.")
	parser.add_option("-c", "--config", dest="configfile", help="Config file name. Defaults to [current_working_directory]/.s3synccfg and attempts %s next" % os.path.join(os.environ['HOME'], '.s3synccfg'))
	parser.add_option("-r", "--run", action="store_true", dest="forcerun", help="Run a sync using the defaults from the default configuration file locations")
	parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="Debugging mode")
	parser.add_option("-d", "--delete", action="store_true", dest="forcedelete", help="Force delete on destination bucket")
	(options, args) = parser.parse_args()

	if len(sys.argv) == 1:
		parser.print_help()
		sys.exit(1)

	s3conf = S3BucketConf(options.configfile)

	if options.configure:
		s3conf.configure()
	elif s3conf.isConfigured():
		if options.forcerun:
			s3 = S3BucketSync(s3conf, options.forcedelete, options.verbose)
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

		s3 = S3BucketSync(s3conf, options.forcedelete, options.verbose)
		s3.sync()
		sys.exit(0)
	else:
		print "ERROR: No configuration files found!  Please use the -C option to setup a configuration file."
		parser.print_help()
		sys.exit(1)
