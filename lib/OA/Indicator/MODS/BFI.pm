package OA::Indicator::MODS::BFI;

use strict;
use warnings;
use OA::Indicator::XML;

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'primary_fields'} = [qw(source source_id pubid doc_type doc_review doc_level class research level fraction point cooperation title lang issn eissn)];
    $self->{'primary_fields1'} = [qw(pubid class research level title lang issn eissn)];
    $self->{'primary_fields2'} = [qw(source source_id doc_type doc_review doc_level fraction point cooperation)];
    $self->{'xpath'} = {
        id         => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:dads:recordid"]',
        year       => '/m:mods/m:relatedItem/m:originInfo[@eventType="publisher"]/m:dateOther',
        pubid      => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:bfi:publication"]',
        class      => '/m:mods/m:subject[@authorityURI="ds.dtic.dk:sub:uncontrolled"]/m:topic',
        research   => '/m:mods/m:subject[@authorityURI="ds.dtic.dk:sub:controlled"]/m:topic[@authorityURI="ds.dtic.dk:sub:bfi:bfi:code"]',
        level      => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:bfi:level"]',
        title      => '/m:mods/m:titleInfo/m:title',
        lang       => '/m:mods/m:language/m:languageTerm',
        issn       => '/m:mods/m:relatedItem/m:identifier[@type="ds.dtic.dk:id:pub:dads:pissn"]',
        eissn      => '/m:mods/m:relatedItem/m:identifier[@type="ds.dtic.dk:id:pub:dads:eissn"]',
        contrib    => '/m:mods/m:extension/d:bfi/d:contributions/d:contribution',
        source     => 'd:recordOriginOrig',
        source_id  => 'd:identifier[@type="ds.dtic.dk:id:pub:dads:sourceid"]',
        doc_type   => 'd:doctype',
        doc_review => 'd:targetAudience[@authorityURI="ds.dtic.dk:review:origin"]',
        doc_level  => 'd:targetAudience[@authorityURI="ds.dtic.dk:level:origin"]',
        fraction   => 'd:fraction',
        point      => 'd:points',
        cooperation=> 'd:cooperation',
    };
    $self->{'xml'} = new OA::Indicator::XML (m => 'http://www.loc.gov/mods/v3', d => 'http://dtic.dk/ds');
    return (bless ($self, $class));
}

sub parse
{
    my ($self, $xml) = @_;

    $self->{'doc'} = $self->{'xml'}->parse ($xml);
    $self->{'rec'} = {};
    foreach my $f (qw(year)) {
        $self->{'rec'}{$f} = $self->field ($f);
    }
}

sub field
{
    my ($self, $name, $doc) = @_;

    my $s = $self->{'xml'}->field ($self->{'xpath'}{$name}, $doc);
    $s =~ s/[\s\t\n\r]+/ /g;
    $s =~ s/^\s//;
    $s =~ s/\s$//;
    return ($s);
}

sub year
{
    my ($self) = @_;

    return ($self->{'rec'}{'year'});
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

    foreach my $f (@{$self->{'primary_fields1'}}) {
        if (!exists ($self->{'rec'}{$f})) {
            $self->{'rec'}{$f} = $self->field ($f);
        }
    }
    foreach my $contrib ($self->{'xml'}->node ($self->{'xpath'}{'contrib'})) {
        my $xpc = new XML::LibXML::XPathContext ($contrib);
        $xpc->registerNs ('d', 'http://dtic.dk/ds');
        foreach my $f (@{$self->{'primary_fields2'}}) {
            $self->{'rec'}{$f} = $self->field ($f, $xpc);
        }
        my $rec = [];
        foreach my $f (@{$self->{'primary_fields'}}) {
            push (@{$rec}, $self->{'rec'}{$f});
        }
        push (@ret, $rec);
    }
    return (@ret);
}

1;

