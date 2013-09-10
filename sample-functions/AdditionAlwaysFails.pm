package AdditionAlwaysFails;

use strict;
use warnings;
use Modern::Perl "2012";

use Moose;
with 'GJS::AbstractFunction';


# Run job
sub run($;$)
{
	my ($self, $args) = @_;

	my $a = $args->{a};
	my $b = $args->{b};

	say STDERR "Going to add $a and $b";

	die "Algebra is hard.";
}


# Return individual job's timeout (0 for no timeout)
sub job_timeout()
{
	# No timeout
	return 0;
}


# Return a number of retries (0 for no retries)
sub retries()
{
	# The job will be attempted 4 times in total
	return 3;
}


# Don't allow two or more jobs with the same parameters to run at once?
sub unique()
{
	return 1;
}


# Notify by email on failure?
sub notify_on_failure()
{
	return 1;
}


no Moose;    # gets rid of scaffolding


# Return package name instead of 1 or otherwise worker.pl won't know the name of the package it's loading
__PACKAGE__;
