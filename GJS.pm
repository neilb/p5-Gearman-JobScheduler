=head1 NAME

C<GJS> - Gearman utilities.

=cut
package GJS;

use YAML qw(LoadFile);

use strict;
use warnings;
use Modern::Perl "2012";

use Gearman::XS qw(:constants);
use Gearman::XS::Client;

# Cancelling jobs:
use IO::Socket::INET;

# Hashref serializing / unserializing
use Data::Compare;
use Storable qw(freeze thaw);
# serialize hashes with the same key order:
$Storable::canonical = 1;


# flush sockets after every write
$| = 1;

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

Returns undef if the job ID was not found; dies on error.

=cut
sub get_gearman_status($$)
{
	my $class = shift;
	my $gearman_job_id = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. GJS->get_gearman_status()";
	}

	my $client = $class->_gearman_client;
	my ($ret, $known, $running, $numerator, $denominator) = $client->job_status($gearman_job_id);

	unless ($ret == GEARMAN_SUCCESS) {
		die "Unable to determine status for Gearman job '$gearman_job_id': " . $client->error();
	}

	unless ($known) {
		# No such job
		return undef;
	}

	my $response = {
		'gearman_job_id' => $gearman_job_id,
		'running' => $running,
		'numerator' => $numerator,
		'denominator' => $denominator
	};
	return $response;
}

=head2 (static) C<cancel_gearman_job($gearman_job_id)>

(Attempt to) cancel a Gearman job.

(None: the job has to be queued and not running.)

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

# Create and return a configured instance of Gearman::Client
sub _gearman_xs_client($)
{
	my $class = shift;

	my $config = $class->_configuration;

	my $client = new Gearman::XS::Client;

	my $ret = $client->add_servers(join(',', @{$config->{servers}}));
	unless ($ret == GEARMAN_SUCCESS) {
		die "Unable to add Gearman servers: " . $client->error();
	}

	$client->set_created_fn(sub {
		my $task = shift;
		say STDERR "Gearman task created: '" . $task->job_handle() . '"';
		return GEARMAN_SUCCESS;
	});

	$client->set_data_fn(sub {
		my $task = shift;
		say STDERR "Data sent to Gearman task '" . $task->job_handle()
		         . "': " . $task->data();
		return GEARMAN_SUCCESS;
	});

	$client->set_status_fn(sub {
		my $task = shift;
		say STDERR "Status updated for Gearman task '" . $task->job_handle()
		         . "': " . $task->numerator()
		         . " / " . $task->denominator();
		return GEARMAN_SUCCESS;
	});

	$client->set_complete_fn(sub {
		my $task = shift;
		say STDERR "Gearman task '" . $task->job_handle()
		         . "' completed with data: " . ($task->data() || '');
		return GEARMAN_SUCCESS;
	});

	$client->set_fail_fn(sub {
		my $task = shift;
		say STDERR "Gearman task failed: '" . $task->job_handle() . '"';
		return GEARMAN_SUCCESS;
	});

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

# Serialize a hashref into string (to be passed to Gearman)
#
# Parameters:
# * hashref that is serializable by Storable module (may be undef)
#
# Returns:
# * a string (string is empty if the hashref is undef)
# 
# Dies on error.
sub _serialize_hashref($$)
{
	my $class = shift;
	my $hashref = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. GJS->_serialize_hashref()";
	}

	unless (defined $hashref) {
		return '';
	}

	unless (ref $hashref eq 'HASH') {
		die "Parameter is not a hashref.";
	}

	# Gearman accepts only scalar arguments
	my $hashref_serialized = undef;
	eval {
		
		$hashref_serialized = freeze $hashref;
		
		# Try to deserialize, see if we get the same hashref
		my $hashref_deserialized = thaw($hashref_serialized);
		unless (Compare($hashref, $hashref_deserialized)) {

			my $error = "Serialized and deserialized hashrefs differ.\n";
			$error .= "Original hashref: " . Dumper($hashref);
			$error .= "Deserialized hashref: " . Dumper($hashref_deserialized);

			die $error;
		}
	};
	if ($@)
	{
		die "Unable to serialize hashref with the Storable module: $@";
	}

	return $hashref_serialized;
}

# Unserialize string (coming from Gearman) back into hashref
#
# Parameters:
# * string to be unserialized; may be empty or undef
#
# Returns:
# * hashref (of the unserialized string), or
# * undef if the string is undef or empty
# 
# Dies on error.
sub _unserialize_hashref($$)
{
	my $class = shift;
	my $string = shift;

	if (ref $class) {
		die "Use this subroutine as a static method, e.g. GJS->_unserialize_hashref()";
	}

	unless ($string) {
		return undef;
	}

	my $hashref = undef;
	eval {
		
		# Unserialize
		$hashref = thaw($string);

		unless (defined $hashref) {
			die "Unserialized hashref is undefined.";
		}

		unless (ref $hashref eq 'HASH') {
			die "Result is not a hashref.";
		}

	};
	if ($@)
	{
		die "Unable to unserialize string with the Storable module: $@";
	}

	return $hashref;
}

1;
