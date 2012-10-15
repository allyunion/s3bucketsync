#!/usr/bin/env python2.6
from boto.s3.connection import S3Connection
from optparse import OptionParser
import sys
import os

"""Purpose sync between buckets, similar to rsync.  This program is copyleft, under the GNU Public License."""

class S3BucketSync:
	def __init__ (self, src, dest, forcedelete, verbose=False):
		self.src = src
		self.dest = dest
		self.verbose = verbose
		self.forcedelete = forcedelete

if __name__ == "__main__":
	parser = OptionParser(usage="%prog [-v] [-d] SOURCE_S3_URL DEST_S3_URL", version="%prog 0.1")
	parser.set_defaults(verbose=False, forcedelete=False)
	parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="Debugging mode")
	parser.add_option("-d", "--delete", action="store_true", dest="forcedelete", help="Force delete on destination bucket")
	(options, args) = parser.parse_args()
	if len(args) != 2:
		print "ERROR: A DEST_S3_URL is required!"
		parser.print_help()
		sys.exit(1)

	usage_exit = False
	for i in args:
		if "s3://" not in i:
			print "ERROR: %s is not a valid s3 URL!" % i
			usage_exit = True

	if usage_exit:
		parser.print_help()
		sys.exit(1)

