=head1 NAME

C<GJS> - Gearman utilities.

=cut
package GJS;

use YAML qw(LoadFile);

use strict;
use warnings;
use Modern::Perl "2012";

use Gearman::Client;

use constant GJS_CONFIG_FILE => 'config.yml';


=head2 (static) C<get_gearman_status($gearman_job_id)>

Get Gearman job status.

Parameters:

=over 4

=item * Gearman job ID (e.g. "127.0.0.1:4730//H:localhost.localdomain:8")

=back

Returns hashref with the job status, e.g.:

=begin text

{
	# Gearman job ID that was passed as a parameter
	'gearman_job_id' => '127.0.0.1:4730//H:tundra.home:8',

	# Whether or not the job is currently running
	'running' => 1,

	# Numerator and denominator of the job's progress
	# (in this example, job is 1333/2000 complete)
	'numerator' => 1333,
	'denominator' => 2000
};

=end text

Returns undef if the job ID was not found.

=cut
sub get_gearman_status($$)
{
	my $class = shift;
	my $gearman_job_id = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. GJS->get_gearman_status()";
	}

	my $client = $class->_gearman_client;
	my $status = $client->get_status($gearman_job_id);

	unless ($status) {
		# No such job?
		return undef;
	}

	my $response = {
		'gearman_job_id' => $gearman_job_id,
		'running' => int($status->[1]),
		'numerator' => int($status->[2]),
		'denominator' => int($status->[3])
	};
	return $response;
}

# Create and return a configured instance of Gearman::Client
sub _gearman_client($)
{
	my $class = shift;

	my $config = $class->_configuration;

	my $client = Gearman::Client->new;
	$client->job_servers(@{$config->{servers}});

	return $client;	
}

# (static) Return configuration, die() on error
sub _configuration($)
{
	my $class = shift;

	my $config = LoadFile(GJS_CONFIG_FILE) or LOGDIE("Unable to read configuration from '" . GJS_CONFIG_FILE . "': $!");
	unless (scalar (@{$config->{servers}})) {
		die "No servers are configured.";
	}

	return $config;	
}

1;
