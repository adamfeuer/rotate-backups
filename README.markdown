Rotate backups script
=====================

This script is designed to be used by processes that create tarred and
compressed backups every hour or every day.  These backups accumulate, taking
up disk space.

By running this rotator script once per hour shortly *before* your hourly backup
cron runs, you can save 24 houly backups, 7 daily backups and an arbitrary
number of weekly backups (the default is 52).

Here's what the script will do:

1. Rename new arrival tarballs to include tarball's mtime date, then move into <username>/hourly/ dir.
2. For any hourly backups which are more than 24 hours old, either move them into daily, or delete
3. For any daily backups which are more than 7 days old, either move them into weekly, or delete
4. Delete excess backups from weekly dir (in excess of user setting: max_weekly_backups)

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

Adam Feuer
adamf@pobox.com
http://adamfeuer.com

Credits
-------

This script is based on the DirectAdmin backup script written by Sean Schertell


