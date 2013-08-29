=head1 NAME

C<GJS> - Gearman utilities.

=cut
package GJS;

use YAML qw(LoadFile);

use strict;
use warnings;
use Modern::Perl "2012";

use Gearman::Client;
use IO::Socket::INET;

# flush sockets after every write
$| = 1;

use constant GJS_CONFIG_FILE => 'config.yml';


# Gearman job ID => GJS job ID mapping
#
# E.g.:
# {
# 	"127.0.0.1:4730//H:tundra.home:7" => "3F44724010DE11E396642DF6156E4EC6.NinetyNineBottlesOfBeer(how_many_bottles_=_2000)",
# 	"127.0.0.1:4730//H:tundra.home:8" => "BACF73BA10DE11E396642DF6156E4EC6.NinetyNineBottlesOfBeer(how_many_bottles_=_100)",
# 	...
# }
my %_gearman_gjs_job_ids;


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

=head2 (static) C<cancel_gearman_job($gearman_job_id)>

(Attempt to) cancel a Gearman job.

Parameters:

=over 4

=item * Gearman job ID (e.g. "127.0.0.1:4730//H:localhost.localdomain:8")

=back

Returns 1 if cancelling was successful, 0 otherwise.

die()s on error.

=cut
sub cancel_gearman_job($$)
{
	my $class = shift;
	my $gearman_job_id = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. GJS->cancel_gearman_job()";
	}

	# Neither Gearman::Client nor Gearman::XS::Client provides a helper
	# subroutine to do this, so we'll have to do this the old way

	my ($server, $internal_job_id) = split('//', $gearman_job_id);
	my ($host, $port) = split(':', $server);

	$port ||= 4730;
	$port = int($port);

	my $socket = new IO::Socket::INET (
	    PeerHost => $host,
	    PeerPort => $port,
	    Proto => 'tcp',
	) or die "Unable to connect to Gearman server: $!\n";

	$socket->send("cancel job " . $internal_job_id . "\r\n");

	my $response = "";
	$socket->recv($response, 1024);
	if ($response ne "OK\r\n") {
		say STDERR "Unable to cancel Gearman job $gearman_job_id";
		$socket->close();
		return 0;
	}

	$socket->close();

	return 1;
}

=head2 (static) C<gearman_job_id_for_gjs_job_id($gjs_job_id)>

Get Gearman job ID for GJS job ID.

(Note: GJS job IDs will be kept in memory only as long as the GJS class is loaded.)

Parameters:

=over 4

=item * Gearman job ID (e.g. "127.0.0.1:4730//H:localhost.localdomain:8")

=back

Returns GJS job ID (e.g. "E1E07D1C10E511E38756ACFE156E4EC6.NinetyNineBottlesOfBeer(how_many_bottles_=_2000)")

Returns undef if the job ID was not found.

=cut
sub gearman_job_id_for_gjs_job_id($$)
{
	my $class = shift;
	my $gjs_job_id = shift;

	my ($gearman_job_id) = grep { $_gearman_gjs_job_ids{$_} eq $gjs_job_id } keys %_gearman_gjs_job_ids;
	return $gearman_job_id;
}

=head2 (static) C<gjs_job_id_for_gearman_job_id($gearman_job_id)>

Get GJS job ID for Gearman job ID.

(Note: GJS job IDs will be kept in memory only as long as the GJS class is loaded.)

Parameters:

=over 4

=item * GJS job ID (e.g. "E1E07D1C10E511E38756ACFE156E4EC6.NinetyNineBottlesOfBeer(how_many_bottles_=_2000)")

=back

Returns Gearman job ID (e.g. "127.0.0.1:4730//H:localhost.localdomain:8")

Returns undef if the job ID was not found.

=cut
sub gjs_job_id_for_gearman_job_id($$)
{
	my $class = shift;
	my $gearman_job_id = shift;

	return $_gearman_gjs_job_ids{$gearman_job_id};
}


sub _register_job_id($$$)
{
	my $class = shift;
	my ($gearman_job_id, $gjs_job_id) = @_;

	$_gearman_gjs_job_ids{$gearman_job_id} = $gjs_job_id;
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
