package OA::Indicator::DS;

use strict;
use warnings;
use JSON::XS;
use Data::Dumper;
use Encode qw(encode decode is_utf8);
use Net::AMQP::RabbitMQ;

sub new
{
    my ($class) = @_;

    my $self = {channel_id => 1, exchange_name => 'cvt.topic', request_id => 'oaindicator', queue_name => 'oa.export',
                queue_opt => {auto_delete => 1, durable => 1}, queue_arg => {'x-max-priority' => 10}, error => {code => 0}};

    return (bless ($self, $class));
}

sub init
{
    my ($self) = @_;

    my $mq = new Net::AMQP::RabbitMQ ();
    $mq->connect('rabbitmq.production.datastore.cvt.dk',
                 {
                     port     =>  5672,
                     user     =>  'ds2',
                     password =>  'B33rBttl',
                     vhost    =>  '/ds2',
                 });
    $mq->channel_open($self->{'channel_id'});
    $mq->basic_qos ($self->{'channel_id'}, {prefetch_count => 20});
    $self->{'queue'} = $mq->queue_declare($self->{'channel_id'}, $self->{'queue_name'}, $self->{'queue_opt'}, $self->{'queue_arg'});
    $mq->consume ($self->{'channel_id'}, $self->{'queue_name'}, {no_ack => 1});
    $mq->queue_bind ($self->{'channel_id'}, $self->{'queue_name'}, $self->{'exchange_name'}, 'export.' . $self->{'request_id'} . '.#');
    $self->{'mq'} = $mq;
}

sub request
{
    my ($self, $set) = @_;

    $self->{'mq'}->publish ($self->{'channel_id'}, 'export_requests',
                            encode_json ({set => $set, request_id => $self->{'request_id'}}),
                            {exchange => $self->{'exchange_name'}});
}

sub record
{
    my ($self) = @_;

    my $msg = $self->{'msg'} = $self->{'mq'}->recv ();
    if ((!$msg->{'body'}) || (length ($msg->{'body'}) == 0)) {
        die ("fatal: Empty payload message body:\n" . Dumper ($msg) . "\n");
    }
    if ($msg->{'body'} eq 'eor') {
        $self->error ('eor', 'done');
        return ({response => 'eor'});
    }
    if (is_utf8 ($msg->{'body'})) {
        $msg->{'body'} = encode ('UTF-8', $msg->{'body'});
    }
    my $rec;
    eval {
        $rec = decode_json ($msg->{'body'});
#       Fields available in the message body:
#           dedupkey      The record's dedupkey
#           pkey          The record's unique datastore id.  Responds to _id in a request (Why is this different)
#           response      'ok'  Static text stating that the record is fine
#           status        'deleted' or 'ok'  If the record deleted in Datastore or available
#           source        The nice name of the source, ie orbit, elsevier, etc. (NB, This is also available in the routing key)
#           timestamp     A Formatted timestamp of the last time the record was updated
#           metadata      The entire record in MODS format
    };
    if ($@) {
        if ($msg->{'body'} eq 'eor') {
            $self->error ('eor', 'done');
            return ({response => 'eor'});
        } elsif ($msg->{'body'} =~ m/^error/i) {
            $self->error ('error', $msg->{'body'});
        } else {
            $self->error ('error', 'undefined error');
        }
        return ({response => 'error'});
    } else {
        return ($rec);
    }
}

sub msg
{
    my ($self) = @_;

    return ($self->{'msg'});
}

sub body
{
    my ($self) = @_;

    return ($self->{'msg'}{'body'});
}

sub error
{
    my ($self, $code, $msg) = @_;

    if (defined ($code)) {
        $self->{'error'} = {code => $code, msg => $msg};
    } else {
        return ($self->{'error'}{'code'});
    }
}

sub error_msg
{
    my ($self) = @_;

    return ($self->{'error'}{'msg'});
}

1;
