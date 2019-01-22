package SendGridEvent;

use strict;
use warnings;
use 5.010;

use Data::Dumper;
use Moo;

use lib "$ENV{HOME}/local_lib/";

use SendGridMessage;

use base qw(
    SendGridTable
);

my %eventTypes = (
    processed         => 1,
    deferred          => 2,
    delivered         => 3,
    open              => 4,
    click             => 5,
    bounce            => 6,
    dropped           => 7,
    spamreport        => 8,
    unsubscribe       => 9,
    group_unsubscribe => 10,
    group_resubscribe => 11,
);

my @_fieldOrder = qw(
    sendGridMessageId
    email
    timestamp
    eventTypeId
    sgEventId
    reason
    response
    status
    useragent
    ip
    url
    asmGroupId
    attempts
);

my @_required = qw(
    email
    sgEventId
);

my %_allFields = map { $_ => undef } @_fieldOrder;

my $_insertSQL =  q{
    INSERT INTO tSendGridEvent
        (}
    . join(', ', @_fieldOrder)
    . q{)
    VALUES
};

my $_placeholderSQL =
      '('
    . join(', ', map {
        $_ eq 'timestamp'
            ? 'FROM_UNIXTIME(?)'
            : '?'
        } @_fieldOrder
    )
    . ')';

sub insertSQL {
    return $_insertSQL;
}

sub placeholderSQL {
    return $_placeholderSQL;
}

has [qw(_id _on)] => (
  is => 'ro',
);

has [@_fieldOrder] => (
  is => 'rwp',
);

sub camelCasifyValidate {
    my ($class, $rRec) = @_;

    foreach my $k (keys %$rRec) {
        my $nk = $k;

        if ($nk =~ s/(?:[_\-])([a-z])/\U$1/g) {
            if (
                   exists $_allFields{$nk}
                || SendGridMessage->existsAllField($nk)
            ) {
                $rRec->{$nk} = delete $rRec->{$k};
            }
        }
    }

    foreach my $k (@_required) {
        $rRec->{$k} //= '';
    }

    # category might be json!; need
    unless (($rRec->{timestamp} // '') =~ /^\d{10}$/) {
        $rRec->{timestamp} = '0000-00-00 00:00:00';

        warn 'Setting zero for invalid timestamp';
    }

    $rRec->{eventTypeId} = $eventTypes{ $rRec->{event} } || 0;
}

my @sizes   = (1000, 100, 10, 1);
my %sizeSQL = (
    map { $_ => $_insertSQL . join(",\n", ($_placeholderSQL)x$_) } @sizes,
);

sub storeAll {
    my ($class, $rRecs) = @_;

    my $incount = 0;
    my $insize  = 0;
    my $rBind   = [];
    my $rMsgIds = {};
    my $remains = scalar @$rRecs;

    foreach my $rRec (@$rRecs) {
        unless ($incount) {
            # determine insert size
            foreach my $size (@sizes) {
                if ($size <= $remains) {
                    $insize = $size;
                    last;
                }
            }

            warn "Size = $insize, Remains = $remains";
        }

        $class->camelCasifyValidate($rRec);

        $rRec->{sendGridMessageId} = SendGridMessage->upsert({
            rMsgIds => $rMsgIds,
            rRec    => $rRec,
        });

        push @$rBind, map { $rRec->{$_} } @_fieldOrder;

        $remains--;

        if (++$incount == $insize) {
            my $sth = $class->dbh()->prepare_cached( $sizeSQL{$insize} );

            unless ($sth->execute(@$rBind)) {
                my $end   = abs($remains + 1 - scalar(@$rRecs));
                my $begin = $end - $insize;

                warn 'Unable to insert records '
                    . "$begin - $end."
                    . Dumper ( @$rRecs[$begin .. $end] );
            }

            $incount = 0;
            $rBind   = [];
        }
    }

    $class->disconnect();
}

1;
