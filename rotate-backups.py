#!/usr/bin/env python

"""
Rotate backups script
=====================

This script is designed to be used by processes that create tarred and
compressed backups every hour or every day.  These backups accumulate, taking
up disk space.

By running this rotator script once per hour shortly *before* your hourly backup
cron runs, you can save 24 hourly backups, 7 daily backups and an arbitrary
number of weekly backups (the default is 52).

Here's what the script will do:

1. Rename new arrival tarballs to include tarball's mtime date, then move into <username>/hourly/ dir.
2. For any hourly backups which are more than 24 hours old, either move them into daily, or delete.
3. For any daily backups which are more than 7 days old, either move them into weekly, or delete.
4. Delete excess backups from weekly dir (in excess of user setting: max_weekly_backups).

This will effectively turn a user_backups dir like this:

backups/
  world.tar.bz2

...into this:

user_backups_archive/
world/
   hourly/
      world-2008-01-01.tar.bz2

Those hourly tarballs will continue to pile up for the first 24 hours, after
which a daily directory will appear.  After 7 days, another directory will
appear for the weekly tarballs as well.

Backups are moved from the incoming arrivals directory to the archives. If you
do not produce hourly backups, but only produce daily backups, they system will
only save the daily backups.


How to install
--------------

1. Place this script somewhere on your server, for example: /usr/local/bin/rotate_backups.py
2. chmod a+x /usr/local/bin/rotate_backups.py
3. Add a cron like this -->  30 * * * * /usr/local/bin/rotate_backups.py > /dev/null

In step three, we added a cronjob for 30 minutes after each hour. This would be
a good setting if for example your backups cron runs every hour on the hour.
It's best to do all your rotating shortly *before* your backups.


How to configure
----------------

You can edit the defaults in the script below, or create a config file in /etc/default/rotate-backups or $HOME/.rotate-backupsrc

The allowed log levels are INFO, WARNING, ERROR, and DEBUG.

The config file format follows the Python ConfigParser format (http://docs.python.org/library/configparser.html). Here is an example:

```
[Settings]
backups_dir = /var/backups/latest/
archives_dir = /var/backups/archives/
hourly_backup_hour = 23
weekly_backup_day = 6
max_weekly_backups = 52
backup_extensions = "tar.gz",".tar.bz2",".jar"
log_level = ERROR
```

Requirements
------------

Python 2.7

(I have not tested this with Python 3)

Contact
-------

If you have comments or improvements, let me know:

Adam Feuer <adamf@pobox.com>
http://adamfeuer.com

License
-------

This script is based on the DirectAdmin backup script written by Sean Schertell.
Modified by Adam Feuer <adamf@pobox.com>
http://adamfeuer.com

License: MIT

Copyright (c) 2011 Adam Feuer

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""

#############################################################################################
# Default Settings                                                                          #
# Note these can also be changed in /etc/default/rotate-backups or $HOME/.rotate-backupsrc  #
#############################################################################################

DEFAULTS = {
            'backups_dir':        '/var/backups/latest/',
            'archives_dir':       '/var/backups/archives/',
            'hourly_backup_hour': 23, # 0-23
            'weekly_backup_day':  6,  # 0-6, Monday-Sunday
            'max_weekly_backups': 52,
            'backup_extensions':  ['tar.gz', '.tar.bz2', '.jar'], # list of file extensions that will be backed up
            'log_level':          'ERROR',
           }

#############################################################################################

import os, sys, time, re, csv, traceback, logging, ConfigParser, StringIO, shutil
from datetime import datetime, timedelta

allowed_log_levels = { 'INFO': logging.INFO, 'ERROR': logging.ERROR, 'WARNING': logging.WARNING, 'DEBUG': logging.DEBUG }

LOGGER = logging.getLogger('rotate-backups')
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(allowed_log_levels[DEFAULTS["log_level"]])
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
LOGGER.addHandler(consoleHandler)


class SimpleConfig(object):
   def __init__(self):
      self.config = ConfigParser.ConfigParser()
      global_configfile = '/etc/default/rotate-backups'
      local_configfile  = os.path.join(os.getenv("HOME"), ".rotate-backupsrc")
      self.config.read([global_configfile, local_configfile])
      log_level = self.config.get('Settings', 'log_level') if self.config.has_section('Settings') else None
      LOGGER.setLevel(allowed_log_levels.get(log_level, DEFAULTS["log_level"]))

   def __getattr__(self, setting):
      r = None

      if self.config.has_section('Settings'):
          if setting in ('hourly_backup_hour', 'weekly_backup_day', 'max_weekly_backups'):
              r = self.config.getint('Settings', setting)
          else:
              r = self.config.get('Settings', setting)

          if setting == 'backup_extensions':
              r = self.parse_extensions(r)

      return r or DEFAULTS.get(setting)

   def parse_extensions(self, extensions_string):
      parser = csv.reader(StringIO.StringIO(extensions_string))
      return list(parser)[0]


class Account(object):
   def __init__(self, account_name):
      self.base_path = '%s/%s/' % (config.archives_dir, account_name)

   def rotate(self, period_name, next_period_name, max_age):
      earliest_creation_date = datetime.now() - max_age
      for backup in self.get_backups_in(period_name):
         if backup.date < earliest_creation_date:
            # This backup is too old, move to other backup directory or delete.
            if next_period_name and backup.is_rotation_time(period_name):
               backup.move_to(next_period_name, config.archives_dir)
            else:
               backup.remove()

   def get_backups_in(self, directory):
      backups = []
      path_to_dir = '%s%s/' % (self.base_path, directory)
      if os.path.isdir(path_to_dir):
         for filename in os.listdir(path_to_dir):
            path_to_file = os.path.join(path_to_dir, filename)
            backups.append(Backup(path_to_file))
      backups.sort()
      return backups


class Backup(object):
   def __init__(self, path_to_file):
      """Instantiation also rewrites the filename if not already done, prepending the date."""
      self.pattern = '(.*)(\-)([0-9]{4}\-[0-9]{2}\-[0-9]{2}\-[0-9]{4})'
      self.path_to_file = path_to_file
      self.filename = self.format_filename()
      self.set_account_and_date(self.filename)

   def set_account_and_date(self, filename):
      match_obj = re.match(self.pattern, filename)
      if match_obj is None:
        return filename
      self.account = match_obj.group(1)
      datestring = match_obj.group(3)
      time_struct = time.strptime(datestring, "%Y-%m-%d-%H%M")
      self.date = datetime(*time_struct[:5])

   def is_rotation_time(self, period_name):
      assert(period_name in ('hourly', 'daily', 'weekly'))

      if period_name == 'hourly':
         actual_time = self.date.hour
         config_time = config.hourly_backup_hour
      elif period_name == 'daily':
         actual_time = self.date.weekday
         config_time = config.weekly_backup_day
      else:
         return False

      if actual_time == config_time:
         LOGGER.debug('%s equals %s.' % (actual_time, config_time))
         return True
      else:
         LOGGER.debug('%s is not %s.' % (actual_time, config_time))
         return False

   def move_to(self, directory, archives_dir):
      destination_dir = os.path.join(archives_dir, self.account, directory);
      new_filepath = os.path.join(archives_dir, self.account, directory, self.filename)
      try:
          LOGGER.info('Moving %s to %s.' % (self.path_to_file, new_filepath))
          if not os.path.isdir(destination_dir):
            os.makedirs(destination_dir)
          shutil.move(self.path_to_file, new_filepath)
      except:
          LOGGER.error('Unable to move latest backups into %s/ directory.' % directory)
          LOGGER.error("Stacktrace: " + traceback.format_exc())
          sys.exit(1)

   def remove(self):
     LOGGER.info('Removing %s' % self.path_to_file)
     os.remove(self.path_to_file)

   def format_filename(self):
      """If this filename hasn't yet been prepended with the date, do that now."""
      # Does the filename include a date?
      path_parts = os.path.split(self.path_to_file)
      filename = path_parts[-1]
      parent_dir = os.sep + os.path.join(*path_parts[:-1])
      if not re.match(self.pattern, filename.split('.')[0]):
          # No date, rename the file.
          self.mtime = time.localtime( os.path.getmtime(self.path_to_file) )
          self.mtime_str = time.strftime('%Y-%m-%d-%H%M', self.mtime)
          account = filename.split('.')[0]
          extension = filename.split('.', 1)[1]
          filename = ('%s-%s.' + extension) % (account, self.mtime_str)
          new_filepath = os.path.join(parent_dir, filename)
          LOGGER.info('Renaming file to %s.' % new_filepath)
          shutil.move(self.path_to_file, new_filepath)
          self.path_to_file = new_filepath
      return filename

   def __cmp__(x, y):
      """For sorting by date."""
      return cmp( x.date, y.date)


def is_backup(filename):
   for extension in config.backup_extensions:
      if filename.endswith(extension):
          return True
   return False

def collect():
   """Return a collection of account objects for all accounts in backup directory."""
   accounts = []
   # Append all account names from archives_dir.
   for account_name in os.listdir(config.archives_dir):
      accounts.append(account_name)
   accounts = sorted(list(set(accounts))) # Uniquify.
   return map(Account, accounts)

def check_dirs():
   # Make sure backups_dir actually exists.
   if not os.path.isdir(config.backups_dir):
      LOGGER.error("Unable to find backups directory: %s." % config.backups_dir)
      sys.exit(1)

   # Make sure archives_dir actually exists.
   if not os.path.isdir(config.archives_dir):
      try:
         os.mkdir(config.archives_dir)
      except:
         LOGGER.error("Unable to create archives directory: %s." % config.archives_dir)
         sys.exit(1)

def rotate_new_arrivals():
   for filename in os.listdir(config.backups_dir):
      if is_backup(filename):
         new_arrival = Backup(os.path.join(config.backups_dir, filename))
         new_arrival.move_to(HOURLY[0], config.archives_dir)

###################################################

config = SimpleConfig()
check_dirs()

#         period_name, next_period_name, max_age):
HOURLY = ('hourly',   'daily',           timedelta(hours = 24))
DAILY  = ('daily',    'weekly',          timedelta(days = 7))
WEEKLY = ('weekly',   '',                timedelta(days = 7 * config.max_weekly_backups))


# For each account, rotate out new_arrivals, old dailies, old weeklies.
rotate_new_arrivals()

for account in collect():
    account.rotate(*HOURLY)
    account.rotate(*DAILY)
    account.rotate(*WEEKLY)

sys.exit(0)
