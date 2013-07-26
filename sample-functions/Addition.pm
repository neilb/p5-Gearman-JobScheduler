package Addition;

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

	unless (defined $a and defined $b) {
		die "Operands 'a' and 'b' must be defined.";
	}

	return $a + $b;
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
	return 3;
}


# Don't allow two or more jobs with the same parameters to run at once?
sub unique()
{
	return 1;
}


# Each job will provide progress reports via $self->progress($numerator, $denominator)?
sub progress_expected()
{
	return 0;
}


no Moose;    # gets rid of scaffolding


# Return package name instead of 1 or otherwise worker.pl won't know the name of the package it's loading
__PACKAGE__;
