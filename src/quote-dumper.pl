#!/usr/bin/env perl

BEGIN {
   my $arch = `uname -m`; chomp($arch);
   push(@INC, "/home/pavlo/.perl-$arch/lib/perl/5.10.0");
   push(@INC, "/home/pavlo/.perl-$arch/share/perl/5.10.0");
   push(@INC, "/home/pavlo/.perl-$arch/lib/perl5");
   push(@INC, "/home/pavlo/Documents/Stocks/collector/modules");
}

use strict;
use warnings;
use Getopt::Long;
use IO::File;
use File::Basename;
use DBI;
use CollectorCommon qw(debug_print);

GetOptions("dbhost=s"    => \$main::opt_dbhost,
           "directory=s" => \$main::opt_directory,
           "limit=s"     => \$main::opt_limit,
           "year=s"      => \$main::opt_year,
           "history"     => \$main::opt_history,
           "debug"       => \$main::opt_debug,
);
$main::opt_directory = "./" unless (defined($main::opt_directory));

my $db = CollectorCommon::db_connect(defined($main::opt_dbhost) ? $main::opt_dbhost : "localhost");


my $stock_table = (defined($main::opt_history) ? "stock_info" : "stocks");
my $data_table  = (defined($main::opt_history) ? "stock_history" : "quotes");
my $stock_id    = (defined($main::opt_history) ? "stock_id" : "id");
my $sql = "SELECT $stock_table.$stock_id AS id, UPPER($stock_table.symbol) AS symbol, $stock_table.name ".
          "  FROM $stock_table, stock_groups ".
          " WHERE (SELECT count(*) FROM $data_table AS q2 ".
          "         WHERE q2.price > 0 ".
          "           AND q2.stock_id = $stock_table.$stock_id) > 0 ".
          "   AND $stock_table.$stock_id = stock_groups.stock_id ".
          "   AND stock_groups.group_id IN (1, 3, 4, 11) ".
          ($#ARGV >= 0 ? " AND $stock_table.symbol IN (".join(",", map { $db->quote($_) } @ARGV).")" : "");
$sql .= " ORDER BY $stock_table.symbol ASC";
$sql .= (defined($main::opt_limit) ? " LIMIT $main::opt_limit" : "");

my $handle = $db->prepare($sql);
$handle->execute() || die $DBI::errstr;
print debug_print($sql) if (defined($main::opt_debug));

## Store the symbol names and counts in a separate file
my $symbol_file = "$main::opt_directory/symbols.txt";
my $symbol_fh = new IO::File(">$symbol_file") || die $!;
printf($symbol_fh "## List of Symbols ($main::opt_year)\n");
printf($symbol_fh "## Andy Pavlo (http://www.cs.brown.edu/~pavlo/)\n");
printf($symbol_fh "\"SYMBOL\",\"NAME\",\"# of Quotes\",\"FIRST DATE\",\"LAST DATE\"\n");
print debug_print("Creating new symbols file '$symbol_file'") if (defined($main::opt_debug));

while (my $row = $handle->fetchrow_hashref()) {
   ## Open up a new file and dump everything we get out of it
   my $symbol = $row->{'symbol'};
   my $id     = $row->{'id'};
   my $name   = $row->{'name'};
   my $count  = 0;
   my $first_date = undef;
   my $last_date = undef;
   
#    print($fh "\# $name \[$symbol\]\n");
#    if (defined($main::opt_history)) {
#       print($fh "\# DATE PRICE OPEN LOW HIGH\n");
#    } else {
#       print($fh "\# DATE PRICE VOLUME ASK ASK_SIZE BID BID_SIZE\n");
#    }

   $sql = "SELECT $data_table.* ".
          "  FROM $data_table ".
          " WHERE $data_table.stock_id = ".$db->quote($id).
          "   AND $data_table.price > 0 ";
   $sql .= " AND cdate >= ".$db->quote($main::opt_year)." AND YEAR(cdate) = ".$db->quote($main::opt_year) if (defined($main::opt_year));
   $sql .= " ORDER BY $data_table.cdate ASC ";
   my $handle2 = $db->prepare($sql);
   print debug_print($sql) if (defined($main::opt_debug));
   $handle2->execute() || die $DBI::errstr;
   my $fh = undef;
   while (my $row = $handle2->fetchrow_hashref()) {
      unless (defined($fh)) {
         my $file = "$main::opt_directory/$symbol.csv";
         $fh = new IO::File(">$file") || die $!;
         print debug_print("Creating new file '$file'") if (defined($main::opt_debug));
         $first_date = $row->{'cdate'};
      }
      $last_date = $row->{'cdate'};
   
      if (defined($main::opt_history)) {
         printf($fh "%s,%0.3f,%d,%0.3f,%0.3f,%0.3f\n",
                     $row->{'cdate'},
                     (defined($row->{'price'})      ? $row->{'price'} : 0),
                     (defined($row->{'volume'})     ? $row->{'volume'} : 0),
                     (defined($row->{'open_price'}) ? $row->{'open_price'} : 0),
                     (defined($row->{'low'})        ? $row->{'low'} : 0),
                     (defined($row->{'high'})       ? $row->{'high'} : 0),
                     $row->{'low'},
                     $row->{'high'});
      } else {
         printf($fh "\"%s\",\"%0.3f\",\"%d\",\"%0.3f\",\"%d\",\"%0.3f\",\"%d\"\n", 
                     $row->{'cdate'},
                     $row->{'price'},
                     $row->{'volume'},
                     $row->{'ask'},
                     $row->{'ask_size'},
                     $row->{'bid'},
                     $row->{'bid_size'});
      }
      $count += 1;
   } # WHILE
   $handle2->finish();
   $fh->close() if (defined($fh));
   
   ## Write out to symbols file
   printf($symbol_fh "\"%s\",\"%s\",\"%d\",\"%s\",\"%s\"\n",
            $symbol,
            $name,
            $count,
            $first_date,
            $last_date) if (defined($first_date));
} # WHILE
$handle->finish();
$symbol_fh->close();

chdir($main::opt_directory);
mkdir("zips");
foreach my $csvfile (glob("*.csv")) {
    my $first = substr(basename($csvfile), 0, 1);
    my $zipfile = undef;
    if ($first =~ /[A-G]/) {
        $zipfile = "A_to_G";
    } elsif ($first =~/[H-P]/) {
        $zipfile = "H_to_P";
    } elsif ($first =~ /[Q-Z]/) {
        $zipfile = "Q_to_Z";
    }
    $zipfile = "zips/$zipfile.zip";
    `zip -g $zipfile $csvfile`;
} # FOREACH


CollectorCommon::shutdown();