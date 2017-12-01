#!/usr/bin/python
# -*- coding: utf-8 -*-

import pyinotify
import sys
import os.path
import urllib
import importlib


from optparse import OptionParser 


class EventHandler(pyinotify.ProcessEvent):

	def __init__(self, verbose = False ):
	
		self.verbose = verbose

	
	def process_IN_CLOSE_WRITE(self, event):
		if self.verbose:
			print "Close_write:", event.pathname
		
		self.process(filename = event.pathname, function = "index-file")


	def process_IN_MOVED_TO(self, event):
		if self.verbose:
			print "Moved_to:", event.pathname

		self.process(filename = event.pathname, function = "index-file")


	def process_IN_MOVED_FROM(self, event):
		if self.verbose:
			print "Moved_from:", event.pathname

		self.process(filename = event.pathname, function = "delete")


	def process_IN_DELETE(self, event):
		if self.verbose:
			print "Delete:", event.pathname

		self.process(filename = event.pathname, function = "delete")


	# run plugins (i.e. mapping) and call API
	def process(self, filename, function):

		# parameters by value (not by reference) so same another files, since plugins are allowed to change them
		parameters = self.config.copy()

		parameters['id'] = filename


		# Run plugins (for example mapping local dirs to remote dirs)
		for plugin in self.config['plugins']:
			
			# start plugin
			if self.verbose:
				print ("Starting plugin {}".format(plugin))
			
			try:
				module = importlib.import_module('etl.' + plugin)
				
				objectreference = getattr(module, plugin, False)
				
				if objectreference:	# if object oriented programming, run instance of object and call its "process" function
					enhancer = objectreference()
				
					parameters, data = enhancer.process(parameters=parameters, data={})

				else:	# else call "process"-function
					functionreference = getattr(module, 'process', False)

					if functionreference:

						parameters, data = functionreference(parameters=parameters, data={})
					
					else:
						sys.stderr.write( "Exception while running plugin {}: Module implements wether object \"{}\" nor function \"process\"\n".format(plugin, plugin) )
	
			# if exception because user interrupted by keyboard, respect this and abbort		
			except KeyboardInterrupt:
				raise KeyboardInterrupt
			# else dont break because fail of a plugin (maybe other plugins or data extraction will success), only error message
			except BaseException as e:
				sys.stderr.write( "Exception while running plugin {}: {}\n".format(plugin, e.message) )
				
				if self.config['raise_pluginexception']:
					raise

			# Abort plugin chain if plugin set parameters['break'] to True
			# (used for example by blacklist or exclusion plugins)
			if 'break' in parameters:
				if parameters['break']:
					break


	
		# if processing aborted (f.e. by blacklist filter)
		abort = False
		if 'break' in parameters:
			if parameters['break']:
				abort = True


		# if not aborted call API URL

		if not abort:

			self.call_api( api = self.config['api'], docid = parameters['id'], function=function )


	def call_api(self, api, docid, function='index-file'):
		
		try:
			parameters = urllib.urlencode( { "uri" : docid } ) 
	
			urllib.urlopen( api + '/' + function + '?' + parameters)

		except BaseException as e:
			sys.stderr.write("Exception while calling API {} for {}:".format(api, docid))
			sys.stderr.write(e.message)
			sys.stderr.write("\n")



class Filemonitor(object):

	def __init__(self, verbose = False ):

		self.verbose=verbose
		
		self.config = {}

		# defaults (if not overwritten in config)

		self.config['api'] = 'http://localhost/search-apps/api'
		
		self.config['plugins'] = []

		self.config['raise_pluginexception'] = False


		self.mask = pyinotify.IN_DELETE | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO | pyinotify.IN_MOVED_FROM  # watched events

		self.watchmanager = pyinotify.WatchManager()  # Watch Manager

		self.handler = EventHandler()
		
		self.notifier = pyinotify.Notifier(self.watchmanager, self.handler)


	def read_configfile(self, configfile):
		result = False
		
		if os.path.isfile(configfile):
			config=self.config
			execfile( configfile, locals() )
			self.config = config
			result = True


	def add_watch(self, filename):

		self.watchmanager.add_watch(filename, self.mask, rec=True, auto_add=True)


	def add_watches_from_file(self, filename):	
		listfile = open(filename)
		for line in listfile:
				filename = line.strip()
				# ignore empty lines and comment lines (starting with #)
				if filename and not filename.startswith("#"):
	
					filemonitor.add_watch(filename)


	def watch(self):

		self.handler.config = self.config
		self.handler.verbose = self.verbose

		self.notifier.loop()



parser = OptionParser("etl-filemonitor [options] filename")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="Print debug messages")
parser.add_option("-c", "--config", dest="config", default=False, help="Config file")
parser.add_option("-f", "--fromfile", dest="fromfile", default=False, help="Config file")

(options, args) = parser.parse_args()


filemonitor = Filemonitor(verbose=options.verbose)

# read config file
if options.config:
	configfile = options.config
else:
	configfile = '/etc/opensemanticsearch/filemonitoring/config'

if os.path.isfile(configfile):
	filemonitor.read_configfile (configfile)
else:
	sys.stderr.write('No (access to) config file {}'.format(configfile))



# add watches for every file/dir given as command line parameter
for filename in args:
	filemonitor.add_watch(filename)


# add watches for every file/dir in list file
if options.fromfile:
	filemonitor.add_watches_from_file(options.fromfile)


# start watching
filemonitor.watch()
