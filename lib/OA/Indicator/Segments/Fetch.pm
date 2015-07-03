package OA::Indicator::Segments::Fetch;

use strict;
use warnings;
use OA::Indicator::DB::MXD;
use OA::Indicator::DB::Fulltext;

sub new
{
    my ($class, $db, $oai) = @_;
    my $self = {};

    $self->{'db'} = $db;
    $self->{'oai'} = $oai;
    return (bless ($self, $class));
}

sub process
{
    my ($self) = @_;

    my $count = {total => 0, requests => 0, noft => 0, notdo => 0, notoa => 0};
    my $records = {};
    my $rs = $self->{'db'}->select ('id', 'records', 'scoped=1 and screened=1');
    my $rec;
    while ($rec = $self->{'db'}->next ($rs)) {
        $count->{'total'}++;
        $records->{$rec->{'id'}} = {id => $rec->{'id'}};
    }
    $self->{'oai'}->log ('i', "starting fulltext checks of $count->{'total'} records");
    my $mxd = new OA::Indicator::DB::MXD ($self->{'db'}, $self->{'oai'});
    my $ft = new OA::Indicator::DB::Fulltext ($self->{'oai'});
    $ft->create ();
    foreach my $id (keys (%{$records})) {
        if (!$mxd->fulltext_exists ($id)) {
            $count->{'noft'}++;
            next;
        }
        if ($mxd->fulltext_type ($id) ne 'digital_object') {
            $count->{'notdo'}++;
            next;
        }
        if ($mxd->fulltext_access ($id) ne 'oa') {
            $count->{'notoa'}++;
            next;
        }
        $count->{'requests'}++;
        $ft->request ($id, $mxd->fulltext_uri ($id), $mxd->fulltext_size ($id), $mxd->fulltext_mime ($id), $mxd->fulltext_filename ($id));
    }
    $self->{'oai'}->log ('i', "placed $count->{'requests'} requests out of $count->{'total'} records");
    $self->{'oai'}->log ('i', "$count->{'noft'} have no fulltext");
    $self->{'oai'}->log ('i', "$count->{'notdo'} are not digital objects");
    $self->{'oai'}->log ('i', "$count->{'notoa'} are not open access");
    $ft->harvest ();
    $self->{'oai'}->log ('i', 'done');
    return (1);
}

1;

