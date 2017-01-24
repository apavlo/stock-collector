#!/usr/local/bin/perl

use strict;
use warnings;

BEGIN {
   my $arch = `uname -m`; chomp($arch);
   push(@INC, "/home/pavlo/.perl-$arch/lib/perl/5.10.0");
   push(@INC, "/home/pavlo/.perl-$arch/share/perl/5.10.0");
   push(@INC, "/home/pavlo/.perl-$arch/lib/perl5");
   push(@INC, "/home/pavlo/Documents/Stocks/collector/modules");
}

use POSIX;
use DBI;
use List::Util;
use Getopt::Long;
use IO::Handle;
use FileHandle;
use Switch;

use CollectorCommon qw(debug_print);
use StockInfo;

##
## Random Sleep Factor
##
my $SLEEP_SEED = 3;
srand();

GetOptions("dbhost=s"   => \$main::opt_dbhost,
           "switches"   => \$main::opt_switches,
           "limit=s"    => \$main::opt_limit,
           "stdout=s"   => \$main::opt_stdout,
           "stderr=s"   => \$main::opt_stderr,
           "group=s"    => \$main::opt_group,
           "debug"      => \$main::opt_debug,
);
if (defined($main::opt_stdout)) {
   open (OUTPUT, ">$main::opt_stdout") || die $!;
   STDOUT->fdopen(\*OUTPUT, "w") || die $!;
}
if (defined($main::opt_stderr)) {
   open (ERROR, ">$main::opt_stderr")   || die $!;
   STDERR->fdopen(\*ERROR, "w")  || die $!;
}
STDOUT->autoflush;
STDERR->autoflush;

$main::opt_dbhost = $ENV{'MYSQL_HOST'} if (!defined($main::opt_dbhost) && defined($ENV{'MYSQL_HOST'}));
my $db = CollectorCommon::db_connect(defined($main::opt_dbhost) ? $main::opt_dbhost : "localhost");

##
## Gather the list of stocks that we want to get information for
##
my $sql = "SELECT stocks.id, stocks.symbol, stocks.name, ".
          "       IF(groups.suffix IS NOT NULL, CONCAT(stocks.symbol, groups.suffix), stocks.symbol) AS full_symbol ".
          "  FROM stocks ".
          "  LEFT OUTER JOIN stock_groups ON stock_groups.stock_id = stocks.id ".
          "  LEFT OUTER JOIN groups ON stock_groups.group_id = groups.id ".
          " WHERE  ";
if ($#ARGV >= 0) {
   $sql .= " stocks.symbol IN (".join(",", map { $db->quote($_) } @ARGV).")"; 
} elsif (defined($main::opt_group)) {
   $sql .= " stock_groups.group_id = ".$db->quote($main::opt_group);
} elsif (defined($main::opt_switches)) {
   $sql .= " stocks.symbol IN (SELECT stocks.symbol FROM stock_switch, stocks LEFT OUTER JOIN stock_groups ON stock_groups.stock_id = stocks.id WHERE stock_groups.stock_id IS NULL AND stocks.id = stock_switch.new_id)";
} else {
   $sql .= " stock_groups.group_id IS NULL";
}
$sql .= " LIMIT $main::opt_limit " if (defined($main::opt_limit));

print debug_print($sql) if (defined($main::opt_debug));
my $handle = $db->prepare($sql);
$handle->execute() || die(debug_print($DBI::errstr));
while (my $row = $handle->fetchrow_hashref()) {
   my $symbol      = $row->{'symbol'};
   my $full_symbol = $row->{'full_symbol'};
   my $stock_id    = $row->{'id'};
   my $name        = $row->{'name'};

   print debug_print("Processing stock '$symbol'") if (defined($main::opt_debug));
   StockInfo::gather($stock_id, $symbol, $full_symbol, $name);

   sleep(int(rand($SLEEP_SEED)) + int(rand($SLEEP_SEED)));
}
$handle->finish();
exit;
