r;

use strict;
use warnings;
use 5.010;

use Data::Dumper;
use Email::Stuffer;
use HTML::Entities qw( encode_entities );
use HTTP::Daemon;
use HTTP::Response;
use JSON;
use Moo;
use Time::Piece;

use lib "$ENV{HOME}/local_lib/";

use SendGridEvent;

use base qw(
    Class::Data::Inheritable
    HTTP::Request
);

__PACKAGE__->mk_classdata(daemon => undef);

has body => (
    is => 'lazy',
);

sub _build_body {
    my ($self) = @_;

    my $body = '<html><body>';

    foreach my $rBounce (
        grep { ($_->{event} // '') eq 'bounce' } @{ $self->payload() }
    ) {
        $body .= q{
            <br/>
            <div style="
                width: 100%; color: #333333;
                background-color: #f5f5f5;
                border: 1px solid #cccccc;
                border-radius: 4px;
                margin: 4px;
            ">
            <table style="border: none;">
        };

        foreach my $k (sort keys %$rBounce) {
            my $txt = '';

            # note, intra-sendGrid messages may not have smtpId
            if ($k =~ /^smtp(?:-)id/i) {
                my $val = $rBounce->{$k};
                $val    =~ s/[<>]//g;
                $val    = encode_entities($val);
                $txt    =
                      q{<a href="https://mail.google.com/mail/u/0/#search/rfc822msgid:}
                    . qq{$val">$val</a>};
            } else {
                $txt = encode_entities($rBounce->{$k});
            }

            $body .= q{
                <tr>
                    <td style="border: none; font-weight:bold;">
            } .  encode_entities($k) . q{
                    </td>
                    <td style="border: none;">
            } .  $txt. q{
                    </td>
                </tr>
            };
        }

        $body .= '</table></div><br/>';
    }

    $body .= '</body></html>';

    return $body;
}

has errors => (
    is => 'rw',
);

sub pushError {
    my ($self, $error) = @_;

    unless ($self->errors()) {
        $self->errors([]);
    }

    if ($error) {
        push @{ $self->errors() }, $error;
    }
}

sub errorCount {
    return scalar @{ $_[0]->errors() // [] };
}

has payload => (
    is => 'lazy',
);

sub _build_payload {
    my ($self) = @_;

    my $rPayload = [];

    eval {
        $rPayload = JSON->new->decode($self->{_content});
    };

    if (!$rPayload && $@) {
        $self->pushError("Unable to parse JSON. $@");

        return [];
    }

    unless (ref($rPayload) eq 'ARRAY') {
        $rPayload = [$rPayload];
    }

    return $rPayload;
}

has response => (
    is => 'lazy',
);

sub _build_response {
    my ($self) = @_;

    my ($code, $text) = (200, 'OK');

    if ($self->errorCount()) {
        $code = 400;
        $text = 'FAIL' . Dumper($self->errors());
    }

    my $response = HTTP::Response->new($code);

    $response->content_type('text/plain');
    $response->content($text);

    return $response;
}


sub do {
    my ($class, $r) = @_;

    my $self = bless $r->clone(), $class;

    SendGridEvent->storeAll($self->payload());

    if (grep { ($_->{event} // '') eq 'bounce' } @{ $self->payload() }) {
        $self->sendEventMessage();
    }

    return $self->response();
}

sub sendEventMessage {
    my ($self) = @_;

    if ($self->body()) {
        if (Email::Stuffer
            ->from(      'sdemuth@fairbanksllc.com' )
            ->to(        'sdemuth@fairbanksllc.com' )
            #->cc(            )
            ->subject(   'SendGrid Bounce!' )
            ->html_body( $self->body() )
            ->send()
        ) {
            return 1;
        } else {
            $self->pushError('Could not send email.');
        }
    }

    return 0;
}

sub start {
    my ($class, $port) = @_;

    unless ($port) {
        die 'Server is not configured.';
    } else {

        my $daemon =
            HTTP::Daemon->new(
                LocalPort => $port
            );

        die 'daemon did not start' . $@ unless $daemon;

        $class->daemon($daemon);

        warn "started on $port";

        while (my $c = $class->daemon()->accept()) {
            while (my $r = $c->get_request()) {
                $c->send_response( $class->do($r) );
            }
        }
    }
}

1;
