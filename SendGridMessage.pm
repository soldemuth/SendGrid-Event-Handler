package SendGridMessage;

use strict;
use warnings;
use 5.010;

use Data::Dumper;
use Moo;

use lib "$ENV{HOME}/local_lib/";

use base qw(
    SendGridTable
);

my @_fieldOrder = qw(
    smtpId
    sgMessageId
    msgType
    stateAbbr
    quarterId
    districtId
    userId
    timeStudyId
);

our %_allFields = map { $_ => undef } @_fieldOrder;

sub existsAllField {
    my ($class, $key) = @_;

    return exists $_allFields{$key};
}

my @_required = qw(
    smtpId
    sgMessageId
    msgType
    stateAbbr
);

my $_insertSQL =  q{
    INSERT INTO tSendGridMessage
        (}
    . join(', ', @_fieldOrder)
    . q{)
    VALUES (}
    . join(', ', map { '?' } @_fieldOrder)
    . q{)
    ON DUPLICATE KEY UPDATE
        sendGridMessageId = LAST_INSERT_ID(sendGridMessageId),
        smtpId            = ?,
        sgMessageId       = ?
    };

has [qw( sendGridMessageId )] => (
  is => 'ro',
);

has [@_fieldOrder] => (
  is => 'rwp',
);

sub upsert {
    my ($class, $rParams) = @_;

    my $rMsgIds = $rParams->{rMsgIds};
    my $rRec    = $rParams->{rRec};
    my $key     = ($rRec->{smtpId} // '') . ($rRec->{sgMessageId} // '');

    unless (exists $rMsgIds->{$key}) {
        foreach my $k (keys %$rRec) {
            my $nk = $k;

            if ($nk =~ s/^FB_//) {
                if ($class->existsAllField($nk)) {
                    $rRec->{$nk} = delete $rRec->{$k};
                }
            }
        }

        foreach my $k (@_required) {
            $rRec->{$k} //= '';
        }

        my @bind = map { $rRec->{$_} } @_fieldOrder;

        push @bind,
            ($rRec->{smtpId} // ''),
            ($rRec->{sgMessageId} // '');

        my $sth = $class->dbh()->prepare_cached($_insertSQL);

        unless ($sth->execute(@bind)) {
            warn 'Unable to upsert ' . Dumper ($rRec);
        } else {
            $rMsgIds->{$key} = $sth->{mysql_insertid};
        }
    }

    return $rMsgIds->{$key} // 0;
}

1;
