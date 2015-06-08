package OA::Indicator::MXD;

use strict;
use warnings;
use OA::Indicator::XML;

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'primary_fields'} = [qw(source_id year type level review)];
    $self->{'xpath'} = {
        source_id  => '/mxd:ddf_doc/@rec_id',
        year       => '/mxd:ddf_doc/@doc_year',
        type       => '/mxd:ddf_doc/@doc_type',
        level      => '/mxd:ddf_doc/@doc_level',
        review     => '/mxd:ddf_doc/@doc_review',
        do         => '/mxd:ddf_doc/mxd:publication/mxd:digital_object',
        do_role    => '@role',
        do_access  => '@access',
        do_size    => 'mxd:file/@size',
        do_mime    => 'mxd:file/@mime_type',
        do_file    => 'mxd:file/@filename',
        do_uri     => 'mxd:uri',
        inet       => '/mxd:ddf_doc/mxd:publication/mxd:inetpub',
        inet_text  => 'mxd:text',
        inet_uri   => 'mxd:uri',
        pissn      => '//mxd:issn[@type="pri"]',
        eissn      => '//mxd:issn[@type="ele"]',
    };
    $self->{'xml'} = new OA::Indicator::XML (mxd => 'http://mx.forskningsdatabasen.dk/ns/documents/1.3');
    return (bless ($self, $class));
}

sub parse
{
    my ($self, $xml) = @_;

    $self->{'doc'} = $self->{'xml'}->parse ($xml);
    $self->{'rec'} = {};
    foreach my $f (qw(source_id)) {
        $self->{'rec'}{$f} = $self->{'xml'}->field ($self->{'xpath'}{$f});
    }
}

sub id
{
    my ($self) = @_;

    return ($self->{'rec'}{'source_id'});
}

sub primary_fields
{
    my ($self) = @_;

    return (@{$self->{'primary_fields'}});
}

sub primary
{
    my ($self) = @_;
    my @ret = ();
    foreach my $f (@{$self->{'primary_fields'}}) {
        if (!exists ($self->{'rec'}{$f})) {
            $self->{'rec'}{$f} = $self->{'xml'}->field ($self->{'xpath'}{$f});
        }
        push (@ret, $self->{'rec'}{$f});
    }
    return (@ret);
}

sub fulltext_fields
{
    return (qw(type uri text role access size mime filename));
}

sub fulltext
{
    my ($self) = @_;
    my @ret = ();

    foreach my $obj ($self->{'xml'}->node ($self->{'xpath'}{'do'})) {
        my $rec = ['digital_object'];
        foreach my $f (qw(do_uri inet_text do_role do_access do_size do_mime do_file)) {
            push (@{$rec}, $self->{'xml'}->field ($self->{'xpath'}{$f}, $obj));
        }
        push (@ret, $rec);
    }
    foreach my $obj ($self->{'xml'}->node ($self->{'xpath'}{'inet'})) {
        my $rec = ['inetpub'];
        foreach my $f (qw(inet_uri inet_text)) {
            push (@{$rec}, $self->{'xml'}->field ($self->{'xpath'}{$f}, $obj));
        }
        push (@ret, $rec);
    }
    return (@ret);
}

sub issn_fields
{
    return ('type', 'issn');
}

sub issn
{
    my ($self) = @_;
    my @ret = ();

    foreach my $issn ($self->{'xml'}->field ($self->{'xpath'}{'pissn'})) {
        push (@ret, ['print', $issn]);
    }
    foreach my $issn ($self->{'xml'}->field ($self->{'xpath'}{'eissn'})) {
        push (@ret, ['electronic', $issn]);
    }
    return (@ret);
}

1;

