package SiliconInvestor;

use strict;
use warnings;
use CollectorCommon qw(clean convert_date);

## ===========================================================
## Investor Hub
## ===========================================================

##
## Since technically these messages don't have titles, we are 
## going to use the titles from the message lists
## We can also get the symbol that the message board is talking about
## from the url
##
my %titles = ( );
my %url_symbols = ( );
my %title_symbols = ( );

##
## Base URL
## 
my $base = "http://siliconinvestor.advfn.com/";
my $ALL_FORUMS_URL = $base."msgboardmain.aspx";

## -----------------------------------------------------------
## groups
## ----------------------------------------------------------
sub groups {
   my @ret = ( );
   ##
   ## Go and find all the groups from the forums list
   ##
   my $html = CollectorCommon::wget_get_url($ALL_FORUMS_URL);
   while ($html =~ m/<tr class='pflist[\w]+'><td><a href='(forum\.aspx\?forumid=[\d]+)'>.*?<\/td>/g) {
      my $url = $base.$1;
      push(@ret, $url) unless (grep(/$url/, @ret));
   } # WHILE
   #@ret = ( "http://siliconinvestor.advfn.com/forum.aspx?forumid=35" );
   return (0, @ret);
}

## -----------------------------------------------------------
## parse
## -----------------------------------------------------------
sub parse {
   my ($html, $symbol, $stock_id) = @_;
   my %data = ( );
   my $timestamp;
   
   ##
   ## Title
   ## Pull the title from our cache if we can get the message id
   ##
   if ($html =~ m/<input name='msgnum' size=[\d]+ value=([\d]+)>/) {
      $data{'title'} = $titles{$1} if (defined($titles{$1}));
   }
   
   ##
   ## Message Date
   ## Get the date in case we ever decide to actually go back and look at this message
   ##
   if ($html =~ m/<td align='right'>([\d]{1,2})\/([\d]{1,2})\/([\d]{4,4}) ([\d]{1,2})\:([\d]{2,2})\:([\d]{2,2}) ([AMP]{2,2})<\/td><\/tr>/) {
      # $1 -> month
      # $2 -> day
      # $3 -> year
      # $4 -> hour
      # $5 -> minute
      # $6 -> second
      # $7 -> am/pm
      ($data{"msg_date"}, $timestamp) = convert_date($6, $5, $4, $7, $2, $1, $3);
   }
   
   ##
   ## There are three ways we can try to get the symbol
   ##    1) See if we belong to a forum that is targetted towards a symbol
   ##    2) Try to pull it from the forum title
   ##    3) Try to pull it from the mesage title
   ##
   unless (defined($symbol)) {
      ##
      ## First figure out our parent forum
      ##
      if ($html =~ m/:[\s]+<a href='(subject\.aspx\?subjectid=[\d]+)'>(.*?)<\/a>/) {
         my $url   = $base.$1;
         my $title = $2;
         if (defined($url_symbols{$url})) {
            $symbol = $url_symbols{$url};
         } elsif (defined($title_symbols{$title})) {
            $symbol = $title_symbols{$title};
         }
      }
      $symbol = CollectorCommon::find_symbol($data{'title'}) unless (defined($symbol));
      unless (defined($symbol)) {
         ## 
         ## There wasn't a symbol that we knew about, so we need to make sure the calling script
         ## marks the url as "No Symbol Found". This is kind of screwy, but this is how
         ## it needs to be done
         ##
         my %data = ( "symbol" => undef );
         my @ret  = ( \%data );
         return (undef, @ret);
      } # UNLESS
      $data{'symbol'} = $symbol;
   } # UNLESS
   ##
   ## User
   ##
   if ($html =~ m/<td>From: <a href='profile\.aspx\?userid=[\d]+'>(.*?)<\/a><\/td>/) {
      $data{'user'} = clean($1);
   }
   ##
   ## Body
   ## I think they have crappy code and forget the span tag when there is no bio
   ##
   if ($html =~ m/<span id="intelliTXT">(.*?)<\/span>/s) {
      $data{'body'} = clean($1);
   } elsif ($html =~ m/<span id="intelliTXT">(.*?)<\/td><\/tr><\/td><\/tr><\/table>/s) {
      $data{'body'} = clean($1);
   }
   
   
   
   ##
   ## Parent URL
   ##
   if ($html =~ m/<td>To: <a href='profile\.aspx\?userid=[\d]+'>.*?<\/a> who wrote \(<a href=(readmsg\.aspx\?msgid=[\d]+)>[\d]+<\/a>\)<\/td>/) {
      $data{"parent_url"} = $base.$1;
      $data{"parent"}     = $CollectorCommon::PARENT_URL;
   }
   
   my @ret = ( );
   push(@ret, \%data);
   #CollectorCommon::debug_dump(%data);
   #exit;
   return ($timestamp, @ret);
}

## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /This website encountered an unexpected problem/);
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /Include Subjects Active in the Past&nbsp;/ ||
           $html =~ /Go to reply\# or date \(mm\/dd\/yy\)\:/);
}

## -----------------------------------------------------------
## geturls
## -------------------------------------------------------
sub geturls {
   my ($html, $symbol, $url) = @_;
   my @ret = ( );

   while ($html =~ m/<td>([\d]+)<\/td><td><a href="(readmsg\.aspx\?msgid=[\d]+)">(.*?)<\/a>/g) {
      my $url   = $base.$2;
      $titles{$1} = CollectorCommon::clean($3);
      push(@ret, $url) unless (grep(/$url/, @ret));   
      #print "MESSAGE: $url\n";
   } # WHILE
   
   ##
   ## There are two types of forums: ones with a symbol and ones without
   ##
   my $stop = undef;
   while ($html =~ m/<td><a href='http.*?\/p\.php\?pid=squote\&symbol=.*?'>(.*?)<\/a><\/td><td><a href='(subject\.aspx\?subjectid=[\d]+)'>(.*?)<\/a><\/td><td align=right>/g) {
      my $symbol = $1;
      my $url    = $base.$2;
      my $title  = CollectorCommon::clean($3);
      $symbol =~ s/\.[\w]+$//;
      #print "FORUM: $2 [$title] ";
      unless (grep(/$url/, @ret)) {
         push(@ret, $url);
         if ($symbol =~ /^[\w\.]+$/) {
            $url_symbols{$url} = $symbol;
            #print "[$symbol]";
         } else {
            $symbol = CollectorCommon::find_symbol($title);
            $title_symbols{$title} = $symbol if (defined($symbol));
            #print "[$symbol]" if (defined($symbol));
         }
      } # UNLESS
      #print "\n";
   } # WHILE
   ##
   ## Figure out whether there are more messages to look for
   ##
   if ($html =~ m/<a href='(subject\.aspx\?subjectid=[\d]+&LastNum=[\d]+&NumMsgs=[\d]+)'>Previous [\d]+<\/a>/) {
      #print "PREV: $base.$1\n";
      push(@ret, $base.$1);
   }
   return (@ret);
}

1;
