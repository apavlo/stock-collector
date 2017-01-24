package HotStockMarket;

use strict;
use warnings;

use CollectorCommon qw(convert_date);

## ===========================================================
## HotStockMarket.com
## ===========================================================

my @GROUPS = ( "penny-stocks-stock-picks" );

##
## URLs to Ignore
## These are the FAQ threads that we don't want to bother with
##
my @IGNORE_URLS = ( "hsm-now-offers-members-gary-witt-trade-like-pro-video-cd",
                    "hsm-200-january-stock-picking-contest-let-fun-begin",
                    "free-hsm-newsletter",
                    "live-chat-now-operational",
                    "microcaptrade-free-tutorial",
                    "forum-rules-regulations-read-before-posting",
);
             
my @EXTRA_URLS = ( );
my @FOUND_URLS = ( );


my $base_url = "http://www.hotstockmarket.com/forums/";

## -----------------------------------------------------------
## groups
## ----------------------------------------------------------
sub groups {
   return (1, @GROUPS);
}

## -----------------------------------------------------------
## parse
## -----------------------------------------------------------
sub parse {
   my ($html, $symbol, $url) = @_;
   my $timestamp;

   my $title      = undef;
   my $extra_urls = undef;
   my @user       = ( );
   my @body       = ( );
   my @msg_date   = ( );
   
   ##
   ## Title
   ## Note that we only use the title to figure out what stock they are talking about
   ## Some of the child messages may divert to another stock, but we assume that
   ## that the discussion is always about the one the topic lists
   ##
   if ($html =~ m/<td class="navbar" style="font-size:[\d]+pt; padding-top:[\d]+px" colspan="[\d]+"><a href=".*?"><img class="inlineimg" src=".*?" alt=".*?" border="[\d]+" \/><\/a>[\s]*<strong>[\n\s\t]+(.*?)[\n\s\t]+<\/strong>/s) {
      $title = CollectorCommon::clean($1);
      chomp($title);
      ##
      ## Find the stock
      ## Bail out if we can't get it
      ##
      $symbol = CollectorCommon::find_symbol($title) unless (defined($symbol));
      unless (defined($symbol)) {
         ##
         ## There wasn't a symbol that we knew about, so we need to make sure the calling script
         ## marks the url as "No Symbol Found". This is kind of screwy, but this is how
         ## it needs to be done
         ##
         my %data = ( "symbol" => undef );
         my @ret  = ( \%data );
         CollectorCommon::shutdown();
         return (undef, @ret);
      } # UNLESS
   }
   
   ##
   ## Now for the rest of the page, there are multiple posts on a single page
   ##
   my @lines = split(/\n/, $html);
   my $idx = -1;
   for (my $ctr = 0; $ctr < $#lines; $ctr++) {
      ##
      ## Date
      ##
      if ($lines[$ctr] =~ m/<a name="post[\d]+"><img class="inlineimg" src=".*?\/forums\/images\/statusicon\/.*?\.gif" alt="[\w\d]+" border="[\d]+" \/><\/a>/) {
         ##
         ## Next couple lines should have what we want
         ##
         for ( ; $ctr < $#lines; $ctr++) {
            if ($lines[$ctr] =~ m/[\s\t]*([A-Z][a-z]{2,2}) ([\d]{1,2})[\w]{2,2}, ([\d]{4,4}), ([\d]{2,2}):([\d]{2,2}) ([APM]{2,2})/) {
               # $1 -> month
               # $2 -> day
               # $3 -> year
               # $4 -> hour
               # $5 -> minute
               ($msg_date[++$idx], $timestamp) = convert_date(0, $5, $4, undef, $2, $1, $3);
               #print "$idx) GOT DATE: $msg_date[$idx]\n";
               last;
            } # IF
         } # FOR
         next;
      }
      ##
      ## User
      ## (?:rel="nofollow")[\s]*
      if ($lines[$ctr] =~ m/<a class="[\w]+username" href="member\.php\?u=[\d]+">(.*?)<\/a>/) {
         ##
         ## Something bad happened if we are getting the user before the message date
         ##
         if ($idx < 0 || defined($user[$idx])) {
            my $subject = "ERROR: Unable to parse HotStockMarket.com";
            my $message = "Found a user before we got the message date for stock $symbol.\n$url";
            CollectorCommon::send_email($subject, $message);
            print debug_print("$subject - Sent email notice!");
            CollectorCommon::shutdown();
         }
         $user[$idx] = CollectorCommon::clean(CollectorCommon::strip_html($1));
         #print "$idx) GOT USER: $user[$idx]\n";
         
         ##
         ## Just plow forward and get our body...
         ##
         my $body = undef;
         for ( ; $ctr < $#lines; $ctr++) {
            if (!defined($body) && $lines[$ctr] =~ m/<\!--[\s]+message[\s]+-->/) {
               $body = $lines[++$ctr];
               next;
            }
            next unless (defined($body));
            if ($lines[$ctr] =~ /<\!--[\s]+\/[\s]+message[\s]+-->/) {
               $body =~ s/[\s\t\n]*<div id="post_message_[\d]+">(.*?)<\/div>/$1/s;
               $body =~ s/<br \/>/ /g;
               ##
               ## Remove quote blocks
               ##
               $body =~ s/<div style="margin:[\d]+px; margin-top:[\d]+px;[\s]+">[\n\s\t]*<div class="smallfont" style="margin-bottom:[\d]+px">Quote:[\n\s\t]*.*?[\n\s\t]*<\/tr>[\n\s\t]*<\/table>[\n\s\t]*<\/div>//gsi;
               ##
               ## Sometimes we get a stray div
               ##
               $body =~ s/<\/div>$//g; # if ($body !~ /<div>/i);
               last;
            } # IF
            $body .= $lines[$ctr];
         } # FOR
         $body[$idx] = CollectorCommon::clean($body) if (defined($body));
      }
   } # FOREACH
   
   ##
   ## Grab the extra URLs to the subsequent pages for this topic
   ## Unlike other sites, there is not "print all" page that allows us to get 
   ## everything we need. Fortunately we have code in place to handle situations like this
   ##
   while ($html =~ m/<a class="smallfont" href="(http\:\/\/www\.hotstockmarket\.com\/forums\/[\w\d\-]+\/[\d]+-[\w\d\-]+-[\d]+\.html)" title="Show results [\d]+ to [\d]+ of [\d,]+">/g) {
      my $url = $1;
      unless (grep(/$url/, @EXTRA_URLS) || grep(/$url/, @FOUND_URLS) || ignore_url($url)) {
         #print "EXTRA: $url\n";
         push(@EXTRA_URLS, $url);
         $extra_urls = 1;
      } # UNLESS
   }
   
   ##
   ## Good to go
   ##
   my @ret = ( );
   for (my $ctr = 0; $ctr <= $idx; $ctr++) {
      my %data = ( "title"     => $title,
                   "symbol"    => $symbol,
                   "body"      => $body[$ctr],
                   "msg_date"  => $msg_date[$ctr],
                   "user"      => $user[$ctr],
      );
      $data{'load_urls'} = 1 if (defined($extra_urls));
      #CollectorCommon::debug_dump(%data);
      push(@ret, \%data);
   }
   #CollectorCommon::shutdown();
   return ($timestamp, @ret);
}
         
## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   return (!defined($html));
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /<td class=\"tcat\" width="[\d]+%">Threads in Forum/);
}

## -----------------------------------------------------------
## geturls
## -----------------------------------------------------------
sub geturls {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   
   ##
   ## We maintain our own list of urls that we have visited because we might be picking up previously
   ## visited pages and there is no way to check whether we have visited a listing page already
   ##
   while ($html =~ m/<a href="(showthread\.php\?t=[\d]+)" id="thread_title_[\d]+">.*?<\/a>/g) {
      #print "[1] $base_url.$1\n";
      unless (grep(/$1/, @ret) || grep(/$1/, @FOUND_URLS) || ignore_url($1)) {
         push(@ret, $base_url.$1);
         push(@FOUND_URLS, $1);
      } # UNLESS
   } # WHILE
   ##
   ## More pages...
   ##
   while ($html =~ m/<a class="[\w\d]+" href="(forumdisplay\.php\?f=[\d]+&amp;page=[\d]+&amp;order=[\w]+)" title="Show results [\d]+ to [\d]+ of [\d,]+"[\s]*>/g) {
      #print "[2] $base_url.$1\n";
      unless (grep(/$1/, @ret) || grep(/$1/, @FOUND_URLS) || ignore_url($1)) {
         push(@ret, $base_url.$1);
         push(@FOUND_URLS, $1);
      } # UNLESS
   }
   ##
   ## Extra URLs
   ##
   foreach my $url (@EXTRA_URLS) {
      unless (grep(/$url/, @ret) || grep(/$url/, @FOUND_URLS) || ignore_url($url)) {
         push(@ret, $url);
         push(@FOUND_URLS, $url);
      } # UNLESS
   }
   @EXTRA_URLS = ( );
   
   return (@ret);
}

sub ignore_url {
   my ($url) = @_;
   for my $pattern (@IGNORE_URLS) {
      return (1) if ($url =~ /$pattern/);
   } # FOR
   return;
}

1;
