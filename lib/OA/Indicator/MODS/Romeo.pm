package OA::Indicator::MODS::Romeo;

use strict;
use warnings;
use OA::Indicator::XML;

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'primary_fields'} = [qw(source_id pissn eissn color publisher title)];
    $self->{'xpath'} = {
        source_id  => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:dads:sourceid"]',
        pissn      => '//m:identifier[@type="ds.dtic.dk:id:pub:dads:pissn"]',
        eissn      => '//m:identifier[@type="ds.dtic.dk:id:pub:dads:eissn"]',
        color      => '/m:mods/m:extension/d:oaindicator/d:romeo/d:romeocolour',
        publisher  => '/m:mods/m:relatedItem/m:originInfo/m:publisher',
        title      => '/m:mods/m:titleInfo/m:title'
    };
    $self->{'xml'} = new OA::Indicator::XML (m => 'http://www.loc.gov/mods/v3', d => 'http://dtic.dk/ds');
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

1;

