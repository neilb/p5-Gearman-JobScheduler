=head1 NAME

C<Gearman::JobScheduler> - Gearman utilities.

=cut
package Gearman::JobScheduler;

$VERSION = '0.02';

use strict;
use warnings;
use Modern::Perl "2012";

use Gearman::JobScheduler::Configuration;

use Gearman::XS qw(:constants);
use Gearman::XS::Client;

# Cancelling jobs:
use IO::Socket::INET;

# Hashref serializing / unserializing
use Data::Compare;
use Data::Dumper;
use Storable qw(freeze thaw);
# serialize hashes with the same key order:
$Storable::canonical = 1;

use Data::UUID;
use Sys::Path;
use File::Path qw(make_path);

use Carp;

use Email::MIME;
use Email::Sender::Simple qw(try_to_sendmail);

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({
	level => $DEBUG,
	utf8=>1,
	layout => "%d{ISO8601} [%P]: %m%n"
});


# flush sockets after every write
$| = 1;

use constant GJS_JOB_ID_MAX_LENGTH => 256;



=head2 (static) C<job_status($gearman_job_id[, $config])>

Get Gearman job status.

Parameters:

=over 4

=item * Gearman job ID (e.g. "H:localhost.localdomain:8")

=item * (optional) Instance of Gearman::JobScheduler::Configuration

=back

Returns hashref with the job status, e.g.:

=begin text

{
	# Gearman job ID that was passed as a parameter
	'gearman_job_id' => 'H:tundra.home:8',

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
sub job_status($;$)
{
	my ($gearman_job_id, $config) = @_;

	unless ($config) {
		$config = Gearman::JobScheduler::_default_configuration();
	}

	my $client = _gearman_xs_client($config);
	my ($ret, $known, $running, $numerator, $denominator) = $client->job_status($gearman_job_id);

	unless ($ret == GEARMAN_SUCCESS) {
		LOGDIE("Unable to determine status for Gearman job '$gearman_job_id': " . $client->error());
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


=head2 (static) C<cancel_gearman_job($gearman_job_id[, $config])>

(Attempt to) cancel a Gearman job.

(None: the job has to be queued and not running.)

Parameters:

=over 4

=item * Gearman job ID (e.g. "H:localhost.localdomain:8")

=item * (optional) Instance of Gearman::JobScheduler::Configuration

=back

Returns 1 if cancelling was successful, 0 otherwise.

die()s on error.

=cut
sub cancel_gearman_job($;$)
{
	my ($gearman_job_handle, $config) = @_;

	unless ($config) {
		$config = Gearman::JobScheduler::_default_configuration();
	}

	my $gearman_job_id = _gearman_job_id_from_handle($gearman_job_handle);

	# Neither Gearman::Client nor Gearman::XS::Client provides a helper
	# subroutine to do this, so we'll have to cancel the job by directly
	# connecting to all the servers
	foreach my $server (@{$config->gearman_servers}) {
		my ($host, $port) = split(':', $server);

		$port ||= 4730;
		$port = int($port);

		my $socket = new IO::Socket::INET (
		    PeerHost => $host,
		    PeerPort => $port,
		    Proto => 'tcp',
		) or LOGDIE("Unable to connect to Gearman server: $!");

		$socket->send("cancel job " . $gearman_job_id . "\r\n");

		my $response = "";
		$socket->recv($response, 1024);
		if ($response ne "OK\r\n") {
			WARN("Unable to cancel Gearman job '$gearman_job_id'");
			$socket->close();
			return 0;
		}

		$socket->close();
	}

	return 1;
}


=head2 (static) C<log_path_for_gearman_job($function_name, $gearman_job_handle[, $config])>

Get a path to where Gearman expects to save the job's log.

(Warning: the job has to running; if not, this subroutine will complain and die.)

Parameters:

=over 4

=item * Function name (e.g. "NinetyNineBottlesOfBeer")

=item * Gearman job ID (e.g. "H:localhost.localdomain:8")

=item * (optional) Instance of Gearman::JobScheduler::Configuration

=back

Returns log path where the job's log is being written, e.g.
"/var/log/gjs/NinetyNineBottlesOfBeer/H_tundra.local_93.NinetyNineBottlesOfBeer().log"

die()s on error.

=cut
sub log_path_for_gearman_job($$;$)
{
	my ($function_name, $gearman_job_handle, $config) = @_;

	unless ($config) {
		$config = Gearman::JobScheduler::_default_configuration();
	}

	# If the job is not running, the log path will not be available
	my $job_status = job_status($gearman_job_handle, $config);
	if ((! $job_status) or (! $job_status->{running})) {
		WARN("Job '$gearman_job_handle' is not running; either it is finished already or hasn't started yet. "
		   . "Thus, the path returned might not yet exist.");
	}

	my $gearman_job_id = _gearman_job_id_from_handle($gearman_job_handle);

	# Sanitize the ID just like run_locally() would
	$gearman_job_id = _sanitize_for_path($gearman_job_id);

	my $log_path_glob = _init_and_return_worker_log_dir($function_name, $config) . $gearman_job_id . '*.log';
	my @log_paths = glob $log_path_glob;

	if (scalar @log_paths == 0) {
		LOGDIE("Log path not found for expression: $log_path_glob");
	}
	if (scalar @log_paths > 1) {
		LOGDIE("Two or more logs found for expression: $log_path_glob");
	}

	return $log_paths[0];
}


# (static) Return an unique, path-safe job name which is suitable for writing
# to the filesystem (e.g. for logging)
#
# Parameters:
# * Gearman function name, e.g. 'NinetyNineBottlesOfBeer'
# * hashref of job arguments, e.g. "{ 'how_many_bottles' => 13 }"
# * (optional) Gearman job ID, e.g.:
#     * "H:tundra.home:18" (as reported by an instance of Gearman::Job), or
#     * "127.0.0.1:4730//H:tundra.home:18" (as reported by gearmand)
#
# Returns: unique job ID, e.g.:
# * "084567C4146F11E38F00CB951DB7256D.NinetyNineBottlesOfBeer(how_many_bottles_=_2000)", or
# * "H_tundra.home_18.NinetyNineBottlesOfBeer(how_many_bottles_=_2000)"
sub _unique_path_job_id($$;$)
{
	my ($function_name, $job_args, $gearman_job_id) = @_;

	unless ($function_name) {
		return undef;
	}

	my $unique_id;
	if ($gearman_job_id) {

		# If Gearman job ID was passed as a parameter, this means that the job
		# was run by Gearman (by running run_on_gearman() or enqueue_on_gearman()).
		# Thus, the job has to be logged to a location that can later be found
		# by knowing the Gearman job ID.

		# Strip the host part (if present)
		$unique_id = _gearman_job_id_from_handle($gearman_job_id);

	} else {

		# If no Gearman job ID was provided, this means that the job is being
		# run locally.
		# The job's output still has to be logged somewhere, so we generate an
		# UUID to serve in place of Gearman job ID.

		my $ug    = new Data::UUID;
		my $uuid = $ug->create_str();	# e.g. "059303A4-F3F1-11E2-9246-FB1713B42706"
		$uuid =~ s/\-//gs;				# e.g. "059303A4F3F111E29246FB1713B42706"

		$unique_id = $uuid;		
	}

	# ID goes first in case the job name shortener decides to cut out a part of the job ID
	my $gjs_job_id = $unique_id. '.' . _unique_job_id($function_name, $job_args);
	if (length ($gjs_job_id) > GJS_JOB_ID_MAX_LENGTH) {
		$gjs_job_id = substr($gjs_job_id, 0, GJS_JOB_ID_MAX_LENGTH);
	}

	# Sanitize for paths
	$gjs_job_id = _sanitize_for_path($gjs_job_id);

	return $gjs_job_id;
}

# (static) Return an unique job ID that will identify a particular job with its
# arguments
#
# * Gearman function name, e.g. 'NinetyNineBottlesOfBeer'
# * hashref of job arguments, e.g. "{ 'how_many_bottles' => 13 }"
#
# Returns: unique job ID, e.g. "NinetyNineBottlesOfBeer(how_many_bottles_=_2000)"
#
# FIXME maybe use Data::Dumper?
sub _unique_job_id($$)
{
	my ($function_name, $job_args) = @_;

	unless ($function_name) {
		return undef;
	}

	# Convert to string
	$job_args = ($job_args and scalar keys $job_args)
		? join(', ', map { "$_ = $job_args->{$_}" } sort(keys $job_args))
		: '';

	return "$function_name($job_args)";
}

sub _sanitize_for_path($)
{
	my $string = shift;

	$string =~ s/[^a-zA-Z0-9\.\-_\(\)=,]/_/gi;

	return $string;
}

# Create and return a configured instance of Gearman::Client
sub _gearman_xs_client($)
{
	my $config = shift;

	my $client = new Gearman::XS::Client;

	unless (scalar (@{$config->gearman_servers})) {
		LOGDIE("No Gearman servers are configured.");
	}

	my $ret = $client->add_servers(join(',', @{$config->gearman_servers}));
	unless ($ret == GEARMAN_SUCCESS) {
		LOGDIE("Unable to add Gearman servers: " . $client->error());
	}

	$client->set_created_fn(sub {
		my $task = shift;
		DEBUG("Gearman task created: '" . $task->job_handle() . '"');
		return GEARMAN_SUCCESS;
	});

	$client->set_data_fn(sub {
		my $task = shift;
		DEBUG("Data sent to Gearman task '" . $task->job_handle() . "': " . $task->data());
		return GEARMAN_SUCCESS;
	});

	$client->set_status_fn(sub {
		my $task = shift;
		DEBUG("Status updated for Gearman task '" . $task->job_handle()
		         . "': " . $task->numerator()
		         . " / " . $task->denominator());
		return GEARMAN_SUCCESS;
	});

	$client->set_complete_fn(sub {
		my $task = shift;
		DEBUG("Gearman task '" . $task->job_handle()
		         . "' completed with data: " . ($task->data() || ''));
		return GEARMAN_SUCCESS;
	});

	$client->set_fail_fn(sub {
		my $task = shift;
		DEBUG("Gearman task failed: '" . $task->job_handle() . '"');
		return GEARMAN_SUCCESS;
	});

	return $client;
}

# Return Gearman job ID from Gearman job handle
#
# Parameters:
# * Gearman job handle, e.g.:
#     * "H:tundra.home:18" (as reported by an instance of Gearman::Job), or
#     * "127.0.0.1:4730//H:tundra.home:18" (as reported by gearmand)
#
# Returns: Gearman job ID (e.g. "H:localhost.localdomain:8")
#
# Dies on error.
sub _gearman_job_id_from_handle($)
{
	my $gearman_job_handle = shift;

	my $gearman_job_id;

	# Strip the host part (if present)
	if (index($gearman_job_handle, '//') != -1) {
		# "127.0.0.1:4730//H:localhost.localdomain:8"
		my ($server, $gearman_job_id) = split('//', $gearman_job_handle);
	} else {
		# "H:localhost.localdomain:8"
		$gearman_job_id = $gearman_job_handle;
	}

	# Validate
	unless ($gearman_job_id =~ /^H:.+?:\d+?$/) {
		LOGDIE("Invalid Gearman job ID: $gearman_job_id");
	}

	return $gearman_job_id;
}

# (static) Initialize (create missing directories) and return a worker log directory path (with trailing slash)
sub _init_and_return_worker_log_dir($$)
{
	my ($function_name, $config) = @_;

	my $worker_log_dir = $config->worker_log_dir;
	unless ($worker_log_dir) {
		LOGDIE("Worker log directory is undefined.");
	}

	# Add a trailing slash
    $worker_log_dir =~ s!/*$!/!;

    # Append the function name
    $worker_log_dir .= _sanitize_for_path($function_name) . '/';

    unless ( -d $worker_log_dir ) {
    	make_path( $worker_log_dir );
    }

    return $worker_log_dir;
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
sub _serialize_hashref($)
{
	my $hashref = shift;

	unless (defined $hashref) {
		return '';
	}

	unless (ref $hashref eq 'HASH') {
		LOGDIE("Parameter is not a hashref.");
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

			LOGDIE($error);
		}
	};
	if ($@)
	{
		LOGDIE("Unable to serialize hashref with the Storable module: $@");
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
sub _unserialize_hashref($)
{
	my $string = shift;

	unless ($string) {
		return undef;
	}

	my $hashref = undef;
	eval {
		
		# Unserialize
		$hashref = thaw($string);

		unless (defined $hashref) {
			LOGDIE("Unserialized hashref is undefined.");
		}

		unless (ref $hashref eq 'HASH') {
			LOGDIE("Result is not a hashref.");
		}

	};
	if ($@)
	{
		LOGDIE("Unable to unserialize string '$string' with the Storable module: $@");
	}

	return $hashref;
}

# Returns default configuration (in case a modified one doesn't exist)
sub _default_configuration()
{
	DEBUG("Will use default configuration");
	return Gearman::JobScheduler::Configuration->new();
}

# Send email to someone; returns 1 on success, 0 on failure
sub _send_email($$$)
{
    my ( $subject, $message, $config ) = @_;

    unless (scalar (@{$config->notifications_emails})) {
    	# No one to send mail to
    	return 1;
    }

	my $from_email = $config->notifications_from_address;
	$subject = ($config->notifications_subject_prefix ? $config->notifications_subject_prefix . ' ' : '' ) . $subject;

	my $message_body = <<"EOF";
Hello,

$message

-- 
Gearman::JobScheduler

EOF

	# DEBUG("Will send email to: " . Dumper($config->notifications_emails));
	# DEBUG("Subject: $subject");
	# DEBUG("Message: $message_body");

	foreach my $to_email (@{$config->notifications_emails})
	{
	    my $email = Email::MIME->create(
	        header_str => [
	            From    => $from_email,
	            To      => $to_email,
	            Subject => $subject,
	        ],
	        attributes => {
	            encoding => 'quoted-printable',
	            charset  => 'UTF-8',
	        },
	        body_str => $message_body
	    );

	    unless ( try_to_sendmail( $email ) )
	    {
	        WARN("Unable to send email to $to_email: $!");
	        return 0;
	    }
	}

	return 1;
}

1;
