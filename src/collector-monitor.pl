#!/usr/bin/env perl
use strict;
use warnings;
use utf8;
binmode STDOUT, ":utf8";

BEGIN {
   my $arch = `uname -m`; chomp($arch);
   push(@INC, "/home/pavlo/.perl-$arch/lib/perl/5.10.0");
   push(@INC, "/home/pavlo/.perl-$arch/share/perl/5.10.0");
   push(@INC, "/home/pavlo/.perl-$arch/lib/perl5");
   push(@INC, "/home/pavlo/Documents/Stocks/collector/modules");
}

use POSIX;
use DBI;
use Cwd;
use Getopt::Long;
use CollectorCommon qw(debug_print);

##
## Output Information
##
my $NUM_OF_STOCKS = "";
my $NUM_OF_STOCKS_CHANGE = "";
my $LAST_BACKUP   = "";
my @TASK_OUTPUT   = ( );
my @TASK_CHANGE   = ( );
my @TASK_LAST     = ( );
my @TASK_IS_RUNNING = ( );

##
## Options
##
$main::opt_html = 0;
GetOptions("dbhost=s"      => \$main::opt_dbhost,
           "backup-dir=s"  => \$main::opt_backup_dir,
           "no-tasks"      => \$main::opt_no_tasks,
           "last-output"   => \$main::opt_last_output,
           "running"       => \$main::opt_is_running,
           "timestamp"     => \$main::opt_timestamp,
           "html"          => \$main::opt_html,
           "debug"         => \$main::opt_debug,
);
unless (defined($main::opt_dbhost)) {
   if (defined($ENV{'MYSQL_HOST'})) {
      $main::opt_dbhost = $ENV{'MYSQL_HOST'};
   } else {
      $main::opt_dbhost = "localhost";
   }
} ## UNLESS
my $db = CollectorCommon::db_connect($main::opt_dbhost);
my $timestamp = time();

##
## Output Items
##
my $ARROW_UP = ($main::opt_html ? '<font style="color: green; font-weight: bold;">▲</font>' : '▲');
my $ARROW_DOWN = ($main::opt_html ? '<font style="color: red; font-weight: bold;">▼</font>' : '▼');
my $PRINTF_FORMAT = '%-13s';
my $TASK_PRINTF_FORMAT = '%-9s';
my $TASK_LIMIT = 1000;
my $TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S';
my $IS_RUNNING_LIMIT = 90; # seconds

## ------------------------------------------------------
## Number of Stocks
## ------------------------------------------------------
my $sql = "SELECT count(*) AS cnt FROM penny_stocks";
print debug_print($sql) if (defined($main::opt_debug));
my $handle = $db->prepare($sql);
$handle->execute() || die(debug_print($DBI::errstr));
if (my $row = $handle->fetchrow_hashref()) {
   $NUM_OF_STOCKS = $row->{'cnt'};
   print debug_print("Stock Count: $NUM_OF_STOCKS") if (defined($main::opt_debug));
   $handle->finish();
}
$sql = "SELECT count(*) AS cnt ".
       "  FROM penny_stocks, stocks ".
       " WHERE penny_stocks.id = stocks.id ".
       "   AND stocks.cdate < ".$db->quote(strftime("%Y-%m-%d", localtime($timestamp)));
print debug_print($sql) if (defined($main::opt_debug));
$handle = $db->prepare($sql);
$handle->execute() || die(debug_print($DBI::errstr));
if (my $row = $handle->fetchrow_hashref()) {
   my $cnt = $row->{'cnt'};
   print debug_print("Yesterday's Stock Count: $cnt") if (defined($main::opt_debug));
   $NUM_OF_STOCKS_CHANGE = sprintf(" (%+d)", ($NUM_OF_STOCKS - $cnt)) if ($cnt < $NUM_OF_STOCKS);
   $handle->finish();
}       

## ------------------------------------------------------
## Last Backup
## ------------------------------------------------------
if (defined($main::opt_backup_dir)) {
   my $newest_file = `ls -1t $main::opt_backup_dir/dump.* | head -n 1`;
   chomp($newest_file);
   $newest_file = Cwd::realpath($newest_file);
   print debug_print("Last Backup: $newest_file") if (defined($main::opt_debug));
   #$LAST_BACKUP = strftime("%b-%d-%Y", localtime((stat($newest_file))[9]));
   $LAST_BACKUP = lastChanged($timestamp, (stat($newest_file))[9]);
}

## ------------------------------------------------------
## Task Output
## ------------------------------------------------------
unless (defined($main::opt_no_tasks)) {
   my $base_sql = "SELECT count(*) AS cnt ".
            "  FROM quotes, task_stocks ".
            " WHERE quotes.stock_id = task_stocks.id ".
            "   AND quotes.cdate >= %s ".
            "   AND quotes.cdate < %s";
   $sql = "CREATE TEMPORARY TABLE task_stocks AS ".
         " SELECT id FROM penny_stocks LIMIT 0";
   print debug_print($sql) if (defined($main::opt_debug));
   $db->do($sql) || die($DBI::errstr);

   my $stock_timestamp = $timestamp;
   ##
   ## If it's before 6:00am, use yesterday
   ##
   my @parts = localtime($stock_timestamp);
   if (strftime("%H", @parts) < 6) {
      $stock_timestamp = mktime(0, 0, 0, $parts[3], $parts[4], $parts[5], 0, 0) - 1;
   }
   print debug_print("Starting analysis timestamp: ".strftime("%Y-%m-%d %H:%M", localtime($stock_timestamp))) if (defined($main::opt_debug));
   my $current_start = strftime("%Y-%m-%d", localtime($stock_timestamp));
   my $current_stop = strftime("%Y-%m-%d %T", localtime($stock_timestamp));
   my $prev_start = $stock_timestamp - 86400;
   my $prev_stop = undef;
   if (strftime("%w", localtime($stock_timestamp - 86400)) == 5) { ## It's Monday, go back to Friday
      $prev_start = strftime("%Y-%m-%d", localtime($stock_timestamp - 259200));
      $prev_stop = strftime("%Y-%m-%d %T", localtime($stock_timestamp - 259200));
   } else {
      $prev_start = strftime("%Y-%m-%d", localtime($stock_timestamp - 86400));
      $prev_stop = strftime("%Y-%m-%d %T", localtime($stock_timestamp - 86400));
   }

   my $job_id = 0;
   for (my $offset = 0; $offset <= $NUM_OF_STOCKS; $offset += $TASK_LIMIT) {
      $timestamp = time();
      my $current_cnt = undef;
      my $prev_cnt = undef;
      my $last_quote = undef;
      my $last_output = undef;
      my $is_running = undef;
      printf("%s\n", "-"x80) if (defined($main::opt_debug));
      print debug_print(sprintf("Job #%02d", $job_id)) if (defined($main::opt_debug));


      ##
      ## Output File Information
      ##
      my $output_file = Cwd::realpath("$ENV{HOME}/Documents/Stocks/jobs/output/stocks/job.$offset.out");
      print debug_print("Output File: $output_file") if (defined($main::opt_debug));
      $last_output = (stat($output_file))[9];
      print debug_print("Last Output: ".strftime("%Y-%m-%d %H:%M", localtime($last_output))) if (defined($main::opt_debug));

      $is_running = ($timestamp - $last_output <= $IS_RUNNING_LIMIT);
      print debug_print("Is Running: ".($timestamp - $last_output)) if (defined($main::opt_debug));
      $last_output = lastChanged(time(), $last_output);

      ##
      ## Database Information
      ##
      $sql = "DELETE FROM task_stocks ";
      print debug_print($sql) if (defined($main::opt_debug));
      $db->do($sql) || die($DBI::errstr);

      $sql = "INSERT INTO task_stocks ".
            "SELECT id ".
            "   FROM penny_stocks ".
            "  ORDER BY symbol ASC ".
            "  LIMIT $TASK_LIMIT ".
            ($offset > 0 ? "OFFSET $offset" : "");
      print debug_print($sql) if (defined($main::opt_debug));
      $db->do($sql) || die($DBI::errstr);

      $sql = sprintf($base_sql, $db->quote($current_start), $db->quote($current_stop));
      print debug_print($sql) if (defined($main::opt_debug));
      $handle = $db->prepare($sql);
      $handle->execute() || die(debug_print($DBI::errstr));
      if (my $row = $handle->fetchrow_hashref()) {
         $current_cnt = $row->{'cnt'};
         $handle->finish();
         print debug_print("Current Quote Count: $current_cnt") if (defined($main::opt_debug));
      }

      $sql = sprintf($base_sql, $db->quote($prev_start), $db->quote($prev_stop));
      print debug_print($sql) if (defined($main::opt_debug));
      $handle = $db->prepare($sql);
      $handle->execute() || die(debug_print($DBI::errstr));
      if (my $row = $handle->fetchrow_hashref()) {
         $prev_cnt = $row->{'cnt'};
         $handle->finish();
         print debug_print("Previous Quote Count: $prev_cnt") if (defined($main::opt_debug));
      }

      unless (defined($main::opt_last_output)) {
         $sql = "SELECT UNIX_TIMESTAMP(MAX(cdate)) AS cdate ".
               "  FROM quotes, task_stocks ".
               " WHERE quotes.stock_id = task_stocks.id ".
               "   AND quotes.cdate <= ".$db->quote($current_stop);
         print debug_print($sql) if (defined($main::opt_debug));
         $handle = $db->prepare($sql);
         $handle->execute() || die(debug_print($DBI::errstr));
         if (my $row = $handle->fetchrow_hashref()) {
            $last_quote = lastChanged($timestamp, $row->{'cdate'});
            print debug_print("Last Quote: ".strftime("%Y-%m-%d %H:%M", localtime($row->{'cdate'}))) if (defined($main::opt_debug));
            $handle->finish();
         }
      }

      push(@TASK_OUTPUT, $current_cnt);
      push(@TASK_CHANGE, ($current_cnt > $prev_cnt ? $ARROW_UP : ($current_cnt < $prev_cnt ? $ARROW_DOWN : "-")));
      push(@TASK_LAST, (defined($main::opt_last_output) ? $last_output : $last_quote));
      push(@TASK_IS_RUNNING, $is_running);

      $job_id++;
   } ## FOR
} # UNLESS

## ------------------------------------------------------
## Output
## ------------------------------------------------------
printf("%s\n", "-"x80) if (defined($main::opt_debug));

if (defined($main::opt_html)) {
   my $base_html = join("", <DATA>);
   my $html = "";

   ## Header
   $html .= sprintf("<B>%s</B> %d%s<BR/>\n", "Stock Count:", $NUM_OF_STOCKS, $NUM_OF_STOCKS_CHANGE);
   $html .= sprintf("<B>%s</B> %s\n",   "Last Backup:", $LAST_BACKUP);
   $html .= "<hr style=\"width: 50%;\"/>\n";

   ## Tasks
   for my $ctr (0 .. $#TASK_OUTPUT) {
      my $job_id = sprintf("Job \#%02d:", $ctr);
      my $line = sprintf("<B>%s</B> %5s", $job_id, $TASK_OUTPUT[$ctr]);
      $line =~ s/\s/&nbsp;/g;
      $line .= sprintf("%s (%s)\n", $TASK_CHANGE[$ctr], $TASK_LAST[$ctr]);
      $html .= "<font color=\"#888888\">" if (defined($main::opt_is_running) && !$TASK_IS_RUNNING[$ctr]);
      $html .= $line;
      $html .= "</font>" if (defined($main::opt_is_running) && !$TASK_IS_RUNNING[$ctr]);
      $html .= "<BR/>\n" unless ($ctr == $#TASK_OUTPUT);
   }

   ## Timestamp
   if (defined($main::opt_timestamp)) {
      $html .= "<p align=\"right\" style=\"padding: 0px; padding-right: 5px;\"><font face=\"sans\" style=\"font-size: 9px; color: #DDDDDD;\">".strftime($TIMESTAMP_FORMAT, localtime(time()))."</font></p>\n";
   }
   printf($base_html, $html);
} else {
   printf("$PRINTF_FORMAT%d%s\n", "Stock Count:", $NUM_OF_STOCKS, $NUM_OF_STOCKS_CHANGE);
   printf("$PRINTF_FORMAT%s\n",   "Last Backup:", $LAST_BACKUP);
   print "\n";
   for my $ctr (0 .. $#TASK_OUTPUT) {
      printf("$TASK_PRINTF_FORMAT%4s%s (%s)\n", "Job \#$ctr:", $TASK_OUTPUT[$ctr], $TASK_CHANGE[$ctr], $TASK_LAST[$ctr]);
   }
} # IF
$db->disconnect();
exit;

## ======================================================
## lastChanged
## ======================================================
sub lastChanged {
   my ($now, $then) = @_;
   my $seconds = $now - $then;
   my @parts = gmtime($seconds);
   my $ret = '';
   if ($parts[7] > 0) {
      $ret = sprintf("%s day%s", $parts[7], ($parts[7] > 1 ? 's' : ''));
   } elsif ($parts[2] > 0) {
      $ret = sprintf("%s hour%s", $parts[2], ($parts[2] > 1 ? 's' : ''));
   } elsif ($parts[1] > 0) {
      $ret = sprintf("%s min%s", $parts[1], ($parts[1] > 1 ? 's' : ''));
   } else {
      $ret = sprintf("%s sec%s", $parts[0], ($parts[0] == 1 ? '' : 's'));
   }
   return ($ret);
}

## ======================================================
## Base HTML Output
## ======================================================
__DATA__
<html>
   <head>
      <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
   </head>
   <body>
   <font size="-1">
%s
   </font>
   </body>
</html>
