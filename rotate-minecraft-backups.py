#!/usr/bin/env python

"""
Rotate Minecraft backups script
===============================

This script is designed to be used with a minecraft service script
(/etc/init.d/minecraft) that tars and compresses the minecraft world directory.
These backups accumulate, taking up disk space.

Running this rotator script one per hour shortly *before* your hourly backup
cron runs, we can save 24 houly backups, seven daily backups and an arbitrary
number of weekly backups.

Here's what the script will do:

1. Rename new arrival tarballs to include tarball's mtime date, then move into <username>/hourly/ dir.
2. For any hourly backups which are more than 24 hours old, either move them into daily, or delete
3. For any daily backups which are more than 7 days old, either move them into weekly, or delete
4. Delete excess backups from weekly dir (in excess of user setting: max_weekly_backups)

This will effectively turn a user_backups dir like this:

backups/
  world.tar.bzip2

...into this:

user_backups_archive/
world/
   hourly/
      world-2008-01-01.tar.bzip2

Those hourly tarballs will continue to pile up for the first 24 hours, after
which a daily directory will appear.  After 7 days, another directory will
appear for the weekly tarballs as well.

How to install
--------------------------------------------------
1. Place this script somewhere on your server, for example: /usr/local/bin/rotate_minecraft_backups.py
2. chmod a+x /usr/local/bin/rotate_minecraft_backups.py 
3. Add a cron like this -->  30 * * * * /usr/local/bin/rotate_minecraft_backups.py > /dev/null

In step three, we added a cronjob for 30 minutes after each hour. This would be
a good setting if for example your backups cron runs every hour on the hour.
It's best to do all your rotating shortly *before* your backups.

This script is based on the DirectAdmin backup script written by Sean Schertell
Modified for Minecraft by Adam Feuer

License: MIT

-----------------------------------------------------

Copyright (c) 2011 Adam Feuer

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


"""


#################################################


# User Settings

backups_dir        = '/home/adamf/minecraft/backups'
archives_dir       = '/home/adamf/minecraft/backups-archives/'
weekly_backup_day  = 6  # 0-6, Monday-Sunday
hourly_backup_hour = 11  # 0-6, Monday-Sunday
max_weekly_backups = 52
backup_extensions    = ['.tar.bzip2', '.jar'] # list of file extensions that will be backed up


#################################################

HOURLY = 'hourly'
DAILY  = 'daily'
WEEKLY = 'weekly'

import os, sys, time, re
from datetime import datetime, timedelta

class Account:

   global backups_dir, archives_dir, weekly_backup_day, max_weekly_backups

   def __init__(self, account_name):
      self.name = account_name
      self.path_to_hourly = '%s/%s/hourly/' % (archives_dir, self.name)
      self.path_to_daily = '%s/%s/daily/' % (archives_dir, self.name)
      self.path_to_weekly = '%s/%s/weekly/' % (archives_dir, self.name)

   @classmethod
   def rotate_new_arrivals(self):
      for filename in os.listdir(backups_dir):
         if is_backup(filename):
            new_arrival = Backup(os.path.join(backups_dir, filename))
            new_arrival.move_to(HOURLY)

   @classmethod
   def collect(object):
      """Return a collection of account objects for all accounts in backup dir"""
      accounts = []
      # Append all account names from archives_dir
      for account_name in os.listdir(archives_dir):
         accounts.append(account_name)
      accounts = sorted(list(set(accounts))) # Uniquify
      return map(Account, accounts)

   def rotate_hourlies(self):
      twelve_hours_ago = datetime.today() - timedelta(hours = 24)
      for hourly in self.get_backups_in(HOURLY):
         if hourly.date < twelve_hours_ago:
            # This houly is more than 24 hours old. Move to daily or delete.
            if hourly.date.hour() == hourly_backup_hour:
               print '%s equals %s' % (hourly.date.hour(), hourly_backup_hour)
               hourly.move_to(DAILY)
            else:
               print '%s is not %s' % (hourly.date.hour(), hourly_backup_hour)
               hourly.remove()
   
   def rotate_dailies(self):
      seven_days_ago = datetime.today() - timedelta(days = 7)
      for daily in self.get_backups_in(DAILY):
         if daily.date < seven_days_ago:
            # This daily is more than seven days old. Move to weekly or delete.
            if daily.date.weekday() == weekly_backup_day:
               print '%s equals %s' % (daily.date.weekday(), weekly_backup_day)
               daily.move_to(WEEKLY)
            else:
               print '%s is not %s' % (daily.date.weekday(), weekly_backup_day)
               daily.remove()
   
   def rotate_weeklies(self):
      expiration_date = datetime.today() - timedelta(days = 7 * max_weekly_backups)
      for weekly in self.get_backups_in(WEEKLY):
         if weekly.date < expiration_date:
            weekly.remove()
   
   def get_backups_in(self, directory):
      backups = []
      path_to_dir = getattr(self, 'path_to_%s' % directory)
      if os.path.isdir(path_to_dir):
         for filename in os.listdir(path_to_dir):
            path_to_file = os.path.join(path_to_dir, filename)
            backups.append(Backup(path_to_file))
      backups.sort()
      return backups
    
class Backup:

   global backups_dir, archives_dir, weekly_backup_day, max_weekly_backups
   
   def __init__(self, path_to_file):
      """Instantiation also rewrites the filename if not already done (prepends date.)"""
      self.path_to_file = path_to_file
      self.filename = self.format_filename()    
      self.parts = self.filename.split('.', 1)
      self.params = self.parts[0].split('-')
      self.account = self.params[0]        
      self.date = self.get_datetime_obj()
   
   def move_to(self, directory):
      new_filepath = os.path.join(archives_dir, self.account, directory, self.filename)
      try:
          print 'moving to %s' % new_filepath
          os.renames(self.path_to_file, new_filepath)
      except:
          print 'Unable move latest backups into %s/ directory.' % directory
          sys.exit(1)
   
   def remove(self):
     print 'Removing %s' % self.path_to_file
     os.remove(self.path_to_file)
   
   
   def format_filename(self):
      """If this filename hasn't yet been prepended with the date, do that now"""
      # Does the filename start with a date?
      path_parts = os.path.split(self.path_to_file)
      filename = path_parts[-1]
      parent_dir = os.sep + os.path.join(*path_parts[:-1])
      if not re.match('(.*)([0-9]{4}\-[0-9]{2}\-[0-9]{2}\-[0-9]{4})', filename.split('.')[0]):
          # No date, rename the file
          self.mtime = time.localtime( os.path.getmtime(self.path_to_file) )
          self.mtime_str = time.strftime('%Y-%m-%d-%H%M', self.mtime)
          account = filename.split('.')[0]
          extension = filename.split('.', 1)[1]
          filename = ('%s-%s.' + extension) % (account, self.mtime_str)
          new_filepath = os.path.join(parent_dir, filename)
          print 'Renaming file to %s' % new_filepath
          os.rename(self.path_to_file, new_filepath)
          self.path_to_file = new_filepath
      return filename
       
   def get_datetime_obj(self):
      year = int(self.params[1])
      month = int(self.params[2])
      day = int(self.params[3])
      hour = int(self.params[4][:2]) 
      minute = int(self.params[4][2:]) 
      return datetime(year, month, day, hour, minute)
   
   def __cmp__(x, y):
      """For sorting by date"""
      return cmp( x.date, y.date)
   
   
def is_backup(filename):
   for extension in backup_extensions:
      if filename.endswith(extension):
          return True
   return False
    
        
###################################################

# Make sure backups_dir actually exists
if not os.path.isdir(backups_dir):
    print "Unable to find backups directory: %s" % backups_dir
    sys.exit(1)

# Make sure archives_dir actually exists
if not os.path.isdir(archives_dir):
    try:
        os.mkdir(archives_dir)
    except:
        print "Unable to create archives directory: %s" % archives_dir
        sys.exit(1)
                
# For each account, rotate out new_arrivals, old dailies, old weeklies

Account.rotate_new_arrivals()

for account in Account.collect():
    account.rotate_hourlies()
    account.rotate_dailies()
    account.rotate_weeklies()
    
sys.exit(0)

