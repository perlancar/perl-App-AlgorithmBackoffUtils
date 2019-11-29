package App::AlgorithmBackoffUtils;

# AUTHOR
# DATE
# DIST
# VERSION

use 5.010001;
use strict 'subs', 'vars';
use warnings;
use Log::ger;

use Algorithm::Backoff::Constant ();
use Algorithm::Backoff::Exponential ();
use Algorithm::Backoff::Fibonacci ();
use Algorithm::Backoff::LILD ();
use Algorithm::Backoff::LIMD ();
use Algorithm::Backoff::MILD ();
use Algorithm::Backoff::MIMD ();
use Time::HiRes qw(time sleep);

my @algos = qw(Constant Exponential Fibonacci LILD LIMD MILD MIMD);
our %SPEC;

our %arg_algorithm = (
    algorithm => {
        summary => 'Backoff algorithm',
        schema => ['str*', in=>\@algos],
        req => 1,
        cmdline_aliases => {a=>{}},
    },
);

our %args_algo_attrs;
for my $algo (@algos) {
    my $args = ${"Algorithm::Backoff::$algo\::SPEC"}{new}{args};
    for my $arg (keys %$args) {
        my $argspec = $args_algo_attrs{$arg} // { %{$args->{$arg}} };
        $argspec->{req} = 0;
        delete $argspec->{pos};
        if ($argspec->{tags} &&
                (grep { $_ eq 'common' || $_ eq 'category:common-to-all-algorithms' } @{ $argspec->{tags} })) {
            $argspec->{tags}[0] = 'category:common-to-all-algorithms';
        } else {
            $argspec->{tags} //= [];
            push @{ $argspec->{tags} }, lc "category:$algo-algorithm";
        }
        $args_algo_attrs{$arg} //= $argspec;
    }
}

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

$SPEC{retry} = {
    v => 1.1,
    summary => 'Retry a command with custom backoff algorithm',
    args => {
        %arg_algorithm,
        %args_retry_common,
        %args_algo_attrs,
    },
    features => {
        dry_run => 1,
    },
    links => [
        {url => 'pm:Algorithm::Backoff::Constant'},
    ],
};
sub retry {
    my %args = @_;

    my $algo = delete $args{algorithm};
    _retry($algo, \%args);
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

$SPEC{retry_lild} = {
    v => 1.1,
    summary => 'Retry a command with LILD (linear increase, linear decrease) backoff',
    args => {
        %args_retry_common,
        %{ $Algorithm::Backoff::LILD::SPEC{new}{args} },
    },
    features => {
        dry_run => 1,
    },
    links => [
        {url => 'pm:Algorithm::Backoff::LILD'},
    ],
};
sub retry_lild {
    _retry("LILD", {@_});
}

$SPEC{retry_limd} = {
    v => 1.1,
    summary => 'Retry a command with LIMD (linear increase, multiplicative decrease) backoff',
    args => {
        %args_retry_common,
        %{ $Algorithm::Backoff::LIMD::SPEC{new}{args} },
    },
    features => {
        dry_run => 1,
    },
    links => [
        {url => 'pm:Algorithm::Backoff::LIMD'},
    ],
};
sub retry_limd {
    _retry("LIMD", {@_});
}

$SPEC{retry_mild} = {
    v => 1.1,
    summary => 'Retry a command with MILD (multiplicative increase, linear decrease) backoff',
    args => {
        %args_retry_common,
        %{ $Algorithm::Backoff::MILD::SPEC{new}{args} },
    },
    features => {
        dry_run => 1,
    },
    links => [
        {url => 'pm:Algorithm::Backoff::MILD'},
    ],
};
sub retry_mild {
    _retry("MILD", {@_});
}

$SPEC{retry_mimd} = {
    v => 1.1,
    summary => 'Retry a command with MIMD (multiplicative increase, multiplicative decrease) backoff',
    args => {
        %args_retry_common,
        %{ $Algorithm::Backoff::MIMD::SPEC{new}{args} },
    },
    features => {
        dry_run => 1,
    },
    links => [
        {url => 'pm:Algorithm::Backoff::MIMD'},
    ],
};
sub retry_mimd {
    _retry("MIMD", {@_});
}

$SPEC{show_backoff_delays} = {
    v => 1.1,
    summary => 'Show backoff delays',
    args => {
        %arg_algorithm,
        %args_algo_attrs,
        logs => {
            summary => 'List of failures or successes',
            schema => ['array*', of=>'str*', 'x.perl.coerce_rules'=>['From_str::comma_sep']],
            'x.name.is_plural' => 1,
            'x.name.singular' => 'log',
            req => 1,
            pos => 0,
            slurpy => 1,
            description => <<'_',

A list of 0's (to signify failure) or 1's (to signify success). Each
failure/success can be followed by `:TIMESTAMP` (unix epoch) or `:+SECS` (number
of seconds after the previous log), or the current timestamp will be assumed.
Examples:

    0 0 0 0 0 0 0 0 0 0 1 1 1 1 1

(10 failures followed by 5 successes).

    0 0:+2 0:+4 0:+6 1

(4 failures, 2 seconds apart, followed by immediate success.)

_
        },
    },
    features => {
        dry_run => 1,
    },
    links => [
        {url => 'pm:Algorithm::Backoff::Fibonacci'},
    ],
};
sub show_backoff_delays {
    my %args = @_;

    my $algo = $args{algorithm} or return [400, "Please specify algorithm"];
    my $algo_args = ${"Algorithm::Backoff::$algo\::SPEC"}{new}{args};

    my %algo_attrs;
    for my $arg (keys %args_algo_attrs) {
        my $argspec = $args_algo_attrs{$arg};
        next unless grep {
            $_ eq 'category:common-to-all-algorithms' ||
            $_ eq lc("category:$algo-algorithm")
        } @{ $argspec->{tags} };
        if (exists $args{$arg}) {
            $algo_attrs{$arg} = $args{$arg};
        }
    }
    #use DD; dd \%args_algo_attrs;
    #use DD; dd \%algo_attrs;
    my $ab = "Algorithm::Backoff::$algo"->new(%algo_attrs);

    my @delays;
    my $time = time();
    my $i = 0;
    for my $log (@{ $args{logs} }) {
        $i++;
        $log =~ /\A([01])(?::(\+)?(\d+))?\z/ or
            return [400, "Invalid log#$i syntax '$log', must be 0 or 1 followed by :TIMESTAMP or :+SECS"];
        if ($2) {
            $time += $3;
        } elsif (defined $3) {
            $time = $3;
        }
        my $delay;
        if ($1) {
            $delay = $ab->success($time);
        } else {
            $delay = $ab->failure($time);
        }
        push @delays, $delay;
    }

    [200, "OK", \@delays];
}

1;
#ABSTRACT: Utilities related to Algorithm::Backoff

=head1 DESCRIPTION

This distributions provides the following command-line utilities:

# INSERT_EXECS_LIST


=head1 append:SEE ALSO

L<Algorithm::Backoff>

=cut
