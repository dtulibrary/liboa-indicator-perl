package OA::Indicator::MODS::DOAJ;

use strict;
use warnings;
use OA::Indicator::XML;

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'primary_fields'} = [qw(source_id pissn eissn license publisher title apc_price apc_currency apc_url)];
    $self->{'xpath'} = {
        source_id    => '/m:mods/m:identifier[@type="ds.dtic.dk:id:pub:dads:sourceid"]',
        pissn        => '//m:identifier[@type="ds.dtic.dk:id:pub:dads:pissn"]',
        eissn        => '//m:identifier[@type="ds.dtic.dk:id:pub:dads:eissn"]',
        license      => '/m:mods/m:extension/d:oaindicator/d:doaj/d:doajlicense',
        publisher    => '/m:mods/m:relatedItem/m:originInfo/m:publisher',
        title        => '/m:mods/m:titleInfo/m:title',
        apc_price    => '/m:mods/m:extension/d:oaindicator/d:doaj/d:apc_average_price',
        apc_currency => '/m:mods/m:extension/d:oaindicator/d:doaj/d:apc_currency',
        apc_url      => '/m:mods/m:extension/d:oaindicator/d:doaj/d:apc_url'
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
        $self->{'rec'}{$f} = $self->field ($f);
    }
}

sub field
{
    my ($self, $name) = @_;

    if (wantarray) {
        my @fld = ();
        foreach my $s ($self->{'xml'}->field ($self->{'xpath'}{$name})) {
            $s =~ s/[\s\t\n\r]+/ /g;
            $s =~ s/^\s//;
            $s =~ s/\s$//;
            if ($s) {
                push (@fld, $s);
            }
        }
        if (@fld) {
            return (@fld);
        } else {
            return ('');
        }
    } else {
        my $s = $self->{'xml'}->field ($self->{'xpath'}{$name});
        $s =~ s/[\s\t\n\r]+/ /g;
        $s =~ s/^\s//;
        $s =~ s/\s$//;
        return ($s);
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
            if ($f =~ m/issn/) {
                $self->{'rec'}{$f} = join (';', $self->field ($f));
            } else {
                $self->{'rec'}{$f} = $self->field ($f);
            }
        }
        push (@ret, $self->{'rec'}{$f});
    }
    return (@ret);
}

1;

