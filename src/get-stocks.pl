#!/usr/bin/env perl
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
use File::Temp;
use Spreadsheet::ParseExcel::Simple;

use CollectorCommon qw(debug_print);

srand();

$main::opt_limit = -1;
GetOptions("dbhost=s"   => \$main::opt_dbhost,
           "limit=s"    => \$main::opt_limit,
           "group=s"    => \@main::opt_groups,
           "stdout=s"   => \$main::opt_stdout,
           "stderr=s"   => \$main::opt_stderr,
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

my $db = CollectorCommon::db_connect(defined($main::opt_dbhost) ? $main::opt_dbhost : "localhost");

##
## For each group that has a list_url, see if we can get new list of stocks
##
my $sql = "SELECT id, name, symbol, list_url ".
          "  FROM groups ".
          " WHERE list_url IS NOT NULL ";
if (defined(@main::opt_groups)) {
   $sql .= " AND groups.id IN (".join(",", map { $db->quote($_) } @main::opt_groups).")";
}
my $stock_ctr = 0;
print debug_print($sql) if (defined($main::opt_debug));
my $handle = $db->prepare($sql);
$handle->execute() || die(debug_print($DBI::errstr));
while (my $row = $handle->fetchrow_hashref()) {
   my $group_list_url = $row->{'list_url'};
   my $group_name     = $row->{'name'};
   my $group_symbol   = $row->{'symbol'};
   my $group_id       = $row->{'id'};
   my @stocks         = ( );
   
   ##
   ## Grab whatever it is at the other end and take it apart!
   ##
   print debug_print("[FETCH] $group_list_url") if (defined($main::opt_debug));
   my $data = CollectorCommon::wget_get_url($group_list_url);
   unless (length($data) > 0) {
      my $subject = "ERROR: Unable collect new stock symbols for $group_name";
      my $message = "We did not retrieve any data for $group_name list URL:\n\n$group_list_url";
      CollectorCommon::send_email($subject, $message);
      next;
   } # UNLESS

   ##
   ## OTC
   ##
   if ($group_symbol eq "OTC BB") {
      foreach my $line (split(/\n/, $data)) {
         my @data = split(/\|/, $line);
         next if (!defined($data[1]) || $data[1] !~ /Common Stock/); 
         my %data = ( "symbol" => $data[0],
                      "name"   => $data[2] );
         $data{"name"} =~ s/(\w+)/\u\L$1/g;
         push(@stocks, \%data);
      } # FOREACH
   ##
   ## Pink Sheets
   ##
   } elsif ($group_symbol eq "Other OTC") {
      my ($fh, $filename) = File::Temp::tempfile("get-stocksXXXX", DIR => "/tmp", UNLINK => 1);
      print debug_print("Writing data to '$filename'") if (defined($main::opt_debug));
      print $fh $data;
      $fh->close();
      my $xls = Spreadsheet::ParseExcel::Simple->read($filename);
      foreach my $sheet ($xls->sheets) {
         while ($sheet->has_data) {
            my @data = $sheet->next_row;
            next unless ($data[4] =~ /Pink Sheets/);
            #print join("|", @data)."\n";
            my %data = ( "symbol" => $data[0],
                         "name"   => $data[1] );
            push(@stocks, \%data);
         } # WHILE
      } # FOREACH
   ##
   ## Unknown!
   ##
   } else {
      CollectorCommon::shutdown("Unable to parse data for group $group_name");
   }
   
   for (my $ctr = 0; $ctr <= $#stocks; $ctr++) {
      my %data = %{$stocks[$ctr]};
      foreach (keys %data) {
         chomp($data{$_});
         $data{$_} =~ s/^\s+|\s+$//g;
      } # FOREACH
      my $sql = "INSERT INTO stocks (".
                "   name, ".
                "   symbol, ".
                "   valid ".
                ") VALUES (".
                $db->quote($data{"name"}).", ".
                $db->quote($data{"symbol"}).", ".
                $db->quote("1").
                ")";
      #print debug_print($sql) if (defined($main::opt_debug));
      my $handle = $db->prepare($sql);
      unless ($handle->execute()) {
         ##
         ## It's not really an error if it's a duplicate entry
         ##
         unless ($DBI::err == $CollectorCommon::DBI_DUPLICATE_ERROR_CODE) { 
            print debug_print($sql) unless (defined($main::opt_debug));
            CollectorCommon::shutdown($DBI::errstr);
         } # UNLESS
      } else {
         print debug_print($sql) if (defined($main::opt_debug));
         my $stock_id = $handle->{mysql_insertid};
         $sql = "INSERT INTO stock_groups VALUES (".
                $db->quote($group_id).", ".
                $db->quote($stock_id).")";
         print debug_print($sql) if (defined($main::opt_debug));
         $db->do($sql) || CollectorCommon::shutdown($DBI::errstr);
         $main::opt_limit--;
         $stock_ctr++;
      } # UNLESS
      $handle->finish();
      last if ($main::opt_limit == 0);
   } # FOR
} # WHILE
$handle->finish();
print debug_print("Added $stock_ctr new stocks") if ($stock_ctr > 0);