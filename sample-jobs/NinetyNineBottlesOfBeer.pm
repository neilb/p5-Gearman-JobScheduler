package NinetyNineBottlesOfBeer;

use strict;
use warnings;
use Modern::Perl "2012";

use Moose;
with 'GJS::AbstractJob';

use Time::HiRes qw(usleep nanosleep);
use Data::Dumper;

# in microseconds
use constant SLEEP_BETWEEN_BOTTLES => 100000;


# Run job
sub run($;$)
{
	my ($self, $args) = @_;

	my $how_many_bottles = $args->{how_many_bottles};
	$how_many_bottles ||= 100;

	# http://www.99-bottles-of-beer.net/language-perl-539.html
	foreach (reverse(1 .. $how_many_bottles)) {
	    my $s = ($_ == 1) ? "" : "s";
	    my $oneLessS = ($_ == 2) ? "" : "s";
	    say STDERR "";
	    say STDERR "$_ bottle$s of beer on the wall,";
	    say STDERR "$_ bottle$s of beer,";
	    say STDERR "Take one down, pass it around,";
	    say STDERR $_ - 1, " bottle${oneLessS} of beer on the wall";

	    usleep(SLEEP_BETWEEN_BOTTLES);
	}
	say STDERR "";
	say STDERR "*burp*";

	say STDOUT "I think I'm done here.";

	return 1;
}


# Return job's timeout (0 for no timeout)
sub job_timeout()
{
	# No timeout
	return 0;
}


# Return a number of retries (0 for no retries)
sub retries()
{
	return 3;
}


# Don't allow two or more tasks with the same parameters to run at once?
sub unique()
{
	return 1;
}



no Moose;    # gets rid of scaffolding

1;
