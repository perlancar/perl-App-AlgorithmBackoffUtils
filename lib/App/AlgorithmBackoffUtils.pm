package App::AlgorithmBackoffUtils;

# DATE
# VERSION

use 5.010001;
use strict 'subs', 'vars';
use warnings;
use Log::ger;

use Algorithm::Backoff::Constant ();
use Algorithm::Backoff::Exponential ();
use Algorithm::Backoff::Fibonacci ();
use Time::HiRes qw(time sleep);

our %SPEC;

our %args_retry_common = (
    command => {
        schema => ['array*', of=>'str*'],
        req => 1,
        pos => 0,
        slurpy => 1,
    },
    retry_on => {
        summary => 'Comma-separated list of exit codes that should trigger retry',
        schema => ['str*', match=>qr/\A\d+(,\d+)*\z/],
        description => <<'_',

By default, all non-zero exit codes will trigger retry.

_
    },
    success_on => {
        summary => 'Comma-separated list of exit codes that mean success',
        schema => ['str*', match=>qr/\A\d+(,\d+)*\z/],
        description => <<'_',

By default, only exit code 0 means success.

_
    },
    skip_delay => {
        summary => 'Do not delay at all',
        schema => 'true*',
        description => <<'_',

Useful for testing, along with --dry-run, when you just want to see how the
retries are done (the number of retries, along with the number of seconds of
delays) by seeing the log messages, without actually delaying.

_
        cmdline_aliases => {D=>{}},
    },
);

sub _retry {
    require IPC::System::Options;

    my ($name, $args) = @_;

    my $mod = "Algorithm::Backoff::$name";
    (my $mod_pm = "$mod.pm") =~ s!::!/!g;
    require $mod_pm;

    my $dry_run    = delete $args->{-dry_run};
    my $command    = delete $args->{command};
    my $retry_on   = delete $args->{retry_on};
    my $success_on = delete $args->{success_on};
    my $skip_delay = delete $args->{skip_delay};

    my $time = time();
    my $ab = $mod->new(%$args);
    my $attempt = 0;
    while (1) {
        $attempt++;
        my ($exit_code, $is_success);
        if ($dry_run) {
            log_info "[DRY-RUN] Executing command %s (attempt %d) ...",
                $command, $attempt;
            $exit_code = -1;
        } else {
            IPC::System::Options::system({log=>1, shell=>0}, @$command);
            $exit_code = $? < 0 ? $? : $? >> 8;
        }
      DETERMINE_SUCCESS: {
            if (defined $retry_on) {
                my $codes = split /,/, $retry_on;
                $is_success = !(grep { $_ == $exit_code } @$codes);
                last;
            }
            if (defined $success_on) {
                my $codes = split /,/, $success_on;
                $is_success = grep { $_ == $exit_code } @$codes;
                last;
            }
            $is_success = $exit_code == 0 ? 1:0;
        }
        if ($is_success) {
            log_trace "Command successful (exit_code=$exit_code)";
            return [200];
        } else {
            my $delay;
            if ($skip_delay) {
                $delay = $ab->failure($time);
            } else {
                $delay = $ab->failure;
            }
            if ($delay == -1) {
                log_error "Command failed (exit_code=$exit_code), giving up";
                return [500, "Command failed (after $attempt attempt(s))"];
            } else {
                log_warn "Command failed (exit_code=$exit_code), delaying %d second(s) before the next attempt ...",
                    $delay;
                sleep $delay unless $skip_delay;
            }
            $time += $delay if $skip_delay;
        }
    }
}

$SPEC{retry_constant} = {
    v => 1.1,
    summary => 'Retry a command with constant delay backoff',
    args => {
        %args_retry_common,
        %{ $Algorithm::Backoff::Constant::SPEC{new}{args} },
    },
    features => {
        dry_run => 1,
    },
    links => [
        {url => 'pm:Algorithm::Backoff::Constant'},
    ],
};
sub retry_constant {
    _retry("Constant", {@_});
}

$SPEC{retry_exponential} = {
    v => 1.1,
    summary => 'Retry a command with exponential backoff',
    args => {
        %args_retry_common,
        %{ $Algorithm::Backoff::Exponential::SPEC{new}{args} },
    },
    features => {
        dry_run => 1,
    },
    links => [
        {url => 'pm:Algorithm::Backoff::Exponential'},
    ],
};
sub retry_exponential {
    _retry("Exponential", {@_});
}

$SPEC{retry_fibonacci} = {
    v => 1.1,
    summary => 'Retry a command with fibonacci backoff',
    args => {
        %args_retry_common,
        %{ $Algorithm::Backoff::Fibonacci::SPEC{new}{args} },
    },
    features => {
        dry_run => 1,
    },
    links => [
        {url => 'pm:Algorithm::Backoff::Fibonacci'},
    ],
};
sub retry_fibonacci {
    _retry("Fibonacci", {@_});
}

1;
#ABSTRACT: Utilities related to Algorithm::Backoff

=head1 DESCRIPTION

This distributions provides the following command-line utilities:

# INSERT_EXECS_LIST


=head1 append:SEE ALSO

L<Algorithm::Backoff>

=cut
