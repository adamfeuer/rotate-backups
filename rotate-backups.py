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

Python 2.7 or Python 3.4

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

import os, sys, time, re, csv, traceback, logging, shutil

PY3 = sys.version_info[0] == 3
if not PY3: import StringIO  # StringIO does not exist in python3


try:
    # 3.x name
    import configparser
except ImportError:
    # 2.x name
    import ConfigParser as configparser


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
      self.config = configparser.ConfigParser()
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
      if PY3:
        parser = csv.reader([extensions_string])
      else:
        parser = csv.reader(StringIO.StringIO(extensions_string))
      return list(parser)[0]


def get_backups_in(account_name, directory, archives_dir):
  base_path = '%s/%s/' % (archives_dir, account_name)
  path_to_dir = '%s%s/' % (base_path, directory)
  backups = []
  if os.path.isdir(path_to_dir):
     for filename in os.listdir(path_to_dir):
        path_to_file = os.path.join(path_to_dir, filename)
        backups.append(Backup(path_to_file))
  backups.sort(key=lambda b: b.date)
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


def is_backup(filename, backup_extensions):
   for extension in backup_extensions:
      if filename.endswith(extension):
          return True
   return False

def collect(archives_dir):
   """Return a collection of account objects for all accounts in backup directory."""
   accounts = []
   # Append all account names from archives_dir.
   for account_name in os.listdir(archives_dir):
      accounts.append(account_name)
   accounts = sorted(list(set(accounts))) # Uniquify.
   return accounts


def check_dirs(backups_dir, archives_dir):
   # Make sure backups_dir actually exists.
   if not os.path.isdir(backups_dir):
      LOGGER.error("Unable to find backups directory: %s." % backups_dir)
      sys.exit(1)

   # Make sure archives_dir actually exists.
   if not os.path.isdir(archives_dir):
      try:
         os.mkdir(archives_dir)
      except:
         LOGGER.error("Unable to create archives directory: %s." % archives_dir)
         sys.exit(1)

def rotate_new_arrivals(backups_dir, archives_dir, backup_extensions, period_name):
   for filename in os.listdir(backups_dir):
      if is_backup(filename, backup_extensions=backup_extensions):
         new_arrival = Backup(os.path.join(backups_dir, filename))
         new_arrival.move_to(period_name, archives_dir)


def is_rotation_time(
  date,
  period_name,
  hourly_backup_hour,
  weekly_backup_day,
):
  assert(period_name in ('hourly', 'daily', 'weekly'))

  if period_name == 'hourly':
     actual_time = date.hour
     config_time = hourly_backup_hour
  elif period_name == 'daily':
     actual_time = date.weekday()
     config_time = weekly_backup_day
  else:
     return False

  if actual_time == config_time:
     LOGGER.debug('%s equals %s.' % (actual_time, config_time))
     return True
  else:
     LOGGER.debug('%s is not %s.' % (actual_time, config_time))
     return False


def rotate(
  account_name,
  period_name,
  next_period_name,
  max_age,
  archives_dir,
  **is_rotation_time_kw
):
  earliest_creation_date = datetime.now() - max_age
  for backup in get_backups_in(
    account_name=account_name,
    directory=period_name,
    archives_dir=archives_dir,
  ):
     if backup.date < earliest_creation_date:
        # This backup is too old, move to other backup directory or delete.
        if next_period_name and is_rotation_time(
          date=backup.date,
          period_name=period_name,
          **is_rotation_time_kw
        ):
           backup.move_to(next_period_name, archives_dir)
        else:
           backup.remove()


try:
  import pytest

  import tempfile


  class TempDirContext(object):
    def __init__(self, **tempfilekwargs):
      self.tempfilekwargs = tempfilekwargs

    def __enter__(self):
      self.dir = tempfile.mkdtemp(**self.tempfilekwargs)
      print('Created {}'.format(self.dir))
      return self.dir

    def __exit__(self, exc_type, exc_value, traceback):
      print('Removing {} and subfolders...'.format(self.dir))
      shutil.rmtree(self.dir)
      print('Removed {}'.format(self.dir))


  def create_basedirs(path):
    basedir = os.path.dirname(path)
    if not os.path.exists(basedir):
      os.makedirs(basedir)


  def create_empty_file(filename):
    assert not os.path.exists(filename)
    create_basedirs(path=filename)
    open(filename, 'a').close()


  def test_rotate_new_arrivals_moves_correctly():
    with TempDirContext(prefix='rotate-backup-tmp') as tmpdir:
      create_empty_file(os.path.join(tmpdir, 'latest/dbdump.tar.bz2'))
      create_basedirs(os.path.join(tmpdir, 'archives'))

      backups_dir = os.path.join(tmpdir, 'latest')
      archives_dir = os.path.join(tmpdir, 'archives')

      assert len(os.listdir(backups_dir)) == 1
      assert not os.path.exists(archives_dir)

      rotate_new_arrivals(
        backups_dir=backups_dir,
        archives_dir=archives_dir,
        backup_extensions=DEFAULTS['backup_extensions'],
        period_name='hourly',
      )

      assert len(os.listdir(backups_dir)) == 0
      assert os.listdir(archives_dir) == ['dbdump']
      assert os.listdir(os.path.join(archives_dir, 'dbdump')) == ['hourly']
      result_files = os.listdir(os.path.join(archives_dir, 'dbdump/hourly'))
      assert result_files[0].startswith('dbdump-')
      assert result_files[0].endswith('.tar.bz2')


  def test_rotate_new_arrivals_ignores_unmatched_files_and_does_not_create_archives_dir():
    with TempDirContext(prefix='rotate-backup-tmp') as tmpdir:
      create_empty_file(os.path.join(tmpdir, 'latest/dbdump.tar.bz22'))
      create_basedirs(os.path.join(tmpdir, 'archives'))

      backups_dir = os.path.join(tmpdir, 'latest')
      archives_dir = os.path.join(tmpdir, 'archives')

      assert len(os.listdir(backups_dir)) == 1
      assert not os.path.exists(archives_dir)

      rotate_new_arrivals(
        backups_dir=backups_dir,
        archives_dir=archives_dir,
        backup_extensions=DEFAULTS['backup_extensions'],
        period_name='hourly'
      )
      assert len(os.listdir(backups_dir)) == 1
      assert not os.path.exists(archives_dir)


  # def test_get_backups_in_returns_backup_objects():
  #   assert 0
  #   ret = get_backups_in(account_name, directory, archives_dir)
  #   assert isinstance(ret[0], Backup)


  # def test_rotate_removes_old_backups():
  #   assert 0


  # def test_rotate_moves_backups():
  #   assert 0


except ImportError:
  pass

###################################################


def do_move_to_archive_and_rotate(
  backups_dir,
  archives_dir,
  backup_extensions,
  max_weekly_backups,
  hourly_backup_hour,
  weekly_backup_day,
  **unused_config
):
  check_dirs(backups_dir=backups_dir, archives_dir=archives_dir)

  # For each account, rotate out new_arrivals, old dailies, old weeklies.
  rotate_new_arrivals(
    backups_dir=backups_dir,
    archives_dir=archives_dir,
    backup_extensions=backup_extensions,
    period_name='hourly',
  )

  for account_name in collect(archives_dir=archives_dir):
    kw = dict(
      archives_dir=archives_dir,
      hourly_backup_hour=hourly_backup_hour,
      weekly_backup_day=weekly_backup_day,
      account_name=account_name,
    )

    rotate(
      period_name='hourly',
      next_period_name='daily',
      max_age=timedelta(hours=24),
      **kw
    )
    rotate(
      period_name='daily',
      next_period_name='weekly',
      max_age=timedelta(days=7),
      **kw
    )
    rotate(
      period_name='weekly',
      next_period_name='',
      max_age=timedelta(days=7*max_weekly_backups),
      **kw
    )


if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument("--noconfig", help="don't look for config file", action="store_true")
  args = parser.parse_args()

  config = SimpleConfig().config.__dict__['_sections']['Settings']
  config['max_weekly_backups'] = int(config['max_weekly_backups'])
  config['hourly_backup_hour'] = int(config['hourly_backup_hour'])
  config['weekly_backup_day'] = int(config['weekly_backup_day'])

  do_move_to_archive_and_rotate(**config)

  sys.exit(0)
