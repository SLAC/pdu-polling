#!/bin/env python

import argparse
import urllib2
from lxml import etree
import logging
from time import time
import telnetlib
import sys

import multiprocessing

mapping = {
    'CPDU-1BM11A.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM11B.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM12A.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM12B.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM13A.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM13B.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM19A.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM20A.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM20B.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM21A.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM21B.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM22A.slac.stanford.edu': 'geist.xml',
    'CPDU-1BM22B.slac.stanford.edu': 'geist.xml',
    'CPDU-1AC18A1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AC18B1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AC25A1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AC25B1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AD25A1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AD25B1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AF18A1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AF18B1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AF25A1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AF25B1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AH18A1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AH18B1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AH25A1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AH25B1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AI18A1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AI18B1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AI25A1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-1AI25B1.slac.stanford.edu': 'servertech.telnet',
    'CPDU-2AX27A.slac.stanford.edu': 'eaton.html',
    'CPDU-2AX27B.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD02A.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD02B.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD03A.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD03B.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD04A.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD04B.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD06A.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD06B.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD07A.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD07B.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD08A.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD08B.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD10A.slac.stanford.edu': 'eaton.html',
    'CPDU-2BD10B.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF37G1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF37G2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF37U1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF37U2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF38G1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF38G2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF38U1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF38U2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF39G1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF39G2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF39U1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF39U2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF40G1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF40G2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF40U1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF40U2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF41G1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF41G2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF41U1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BF41U2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ37G1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ37G2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ37U1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ37U2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ38G1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ38G2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ38U1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ38U2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ39G1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ39G2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ39U1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ39U2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ40G1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ40G2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ40U1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ40U2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ41G1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ41G2.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ41U1.slac.stanford.edu': 'eaton.html',
    'CPDU-2BJ41U2.slac.stanford.edu': 'eaton.html',
}


def fetch( url, type='xml', timeout=5 ): # or html
    doc = None
    try:
        fp = urllib2.urlopen(url, timeout=timeout)
        if type == 'html':
            doc = etree.parse( fp, etree.HTMLParser() )
        else:
            doc = etree.parse(fp)
        # print etree.tostring( doc.getroot(), pretty_print=True )
        fp.close()
    except:
        pass
    return doc


def eaton_html( device, xpath="//table/tr", sub_xpath="./td/text()" ):
    tree = fetch( 'http://%s' % device, type='html' )
    if tree:
        for t in tree.xpath( xpath ):
            # print etree.tostring( t, pretty_print=True )
            data = [ x for x in t.xpath( sub_xpath ) ]
            # print "data: %s" % data
            if len(data):
                name = data[1].strip()
                value = float(data[2].strip())
                # metric = 'current'
                # conver to C
                if 'Temp' in name:
                    value = ( value - 32. ) *  5. / 9.
                    yield {
                        'name': name,
                        'temp': value,
                    }
                else:
                    yield {
                        'name': name,
                        'current': value,
                    }

def geist_xml( device, xpath='/server/devices/device/field' ):
    tree = fetch( 'http://%s/data.xml' % device )
    if tree:
        total_current = 0
        for f in tree.xpath( xpath ):
            # logging.error(" %s" % f)
            this_current = float(f.attrib['value'])
            total_current = total_current + this_current
            yield {
                'name': f.attrib['key'],
                'current': this_current,
            }
        # return sum as name 'Input' (as per eatons)
        yield {
            'name': 'Input',
            'current': total_current,
        }
        
def servertech_telnet( device, user='admn', password='admn', max_wait=3 ):
    tn = telnetlib.Telnet(device, 23, 5)
    tn.read_until( "Username: ", max_wait )
    tn.write( user + "\n" )
    tn.read_until("Password: ", max_wait )
    tn.write( password + "\n" )

    tn.read_until("Smart CDU: ", max_wait )
    tn.write( "istat" + "\n")
    
    total_current = 0
    total_power = 0
    for line in tn.read_until("Smart CDU: ", max_wait ).split("\n"):
        try:
            # logging.error("> LINE: %s" % (line,))
            a = line.split()
            if a[0].startswith('.'):
                # logging.debug(">  %s" % (a,))
                
                try:
                    total_current = total_current + float(a[3]);
                except:
                    pass
                try:
                    total_power = total_power + float(a[5]);
                except:
                    pass
                    
                yield {
                    'name': a[1],
                    'state': True if a[2] == 'On' else False,
                    'current': a[3],
                    'voltage': a[4],
                    'power': a[5],
                }
        except:
            pass

    # totals
    if total_current > 0:
        yield {
            'name': 'Input',
            'current': total_current,
            'power': total_power,
        }

    tn.close()
    

def map_data( device, iterator, table='cpdu', epoch=None ):
    # 'cpdu device=blah,item="Branch 1" current=1.0'
    # 'cpdu device=blah,item="Branch 2" current=2.0'
    # 'cpdu device=blah,item="Temp 2" temp=2.0'
    data = []
    for d in iterator:
        try:
            # logging.error("===: %s" % (d,))
            # remove spaces from item name
            item = d['name'].replace(' ','_')
            x = []
            for k,v in d.iteritems():
                # map booleans
                if isinstance(v,bool):
                    v = '%s' % v
                    v = v.lower()
                if not k == 'name':
                    x.append( "%s=%s" % (k,v) )
            s = '%s,device=%s,item=%s %s %s' % (table, device,item, ','.join(x), '%s'%epoch if epoch else '' )
            # logging.error(" >: %s" % (s,) )
            data.append(s)
        except:
            pass
    return data
        
        

def send_to_influxdb( data, server='influxdb01.slac.stanford.edu', port=8086, db='dc', precision='s', user=None, password=None ):
    userpass = ''
    if user and password:
        userpass = '&u=%s&p=%s' % ( user, password )
    url = 'http://%s:%s/write?db=%s%s&precision=%s' % ( server, port, db, userpass, precision )
    array = []
    for k,v in data.iteritems():
        for a in v:
            array.append( a )
    logging.info('sending to %s' % url )
    logging.info(" data\n%s" % '\n'.join(array) )
    try:
        resp = urllib2.urlopen( url, '\n'.join( array ) )
        code = resp.getcode()
    except urllib2.HTTPError as e:
        logging.error("influx error: %s" % (e.read(),) )

def print_as_influxdb( data ):
    array = []
    for k,v in data.iteritems():
        for a in v:
            print( a )


def worker( device, driver, output ):
    """
    actually go and get data and organise it ready for sending to influx; store the data into the output array
    """
    data = []
    try:
        # generate iterator function for data output
        function = driver( device )
        for l in map_data( device, function, epoch=None ): #int(time()) ):
            # logging.error("+ %s -> %s" % (device,l,))
            data.append( l )
        output[device] = data
    except Exception, e:
        logging.debug("Error: %s %s" % (device,e))

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser( description="collects pdu statistics")
    
    parser.add_argument( '-v', '--verbose', help='verbosity', action='store_true', default=False )
    # parser.add_argument( '-w', '--workers', help='number of concurrent workers', type=int, default=2 )
    parser.add_argument( '--format', help='log format', default="%(asctime)s %(message)s" )
    parser.add_argument( '--level', help='logging level', default='INFO' )
    
    parser.add_argument( 'device', help='query specific device', default=[], nargs='*' )
    parser.add_argument( '--driver', help='query with specific driver', choices=[ 'geist.xml', 'eaton.html', 'servertech.telnet' ], default='servertech.telnet' )

    parser.add_argument( '--stdout', help='print as stdout', action='store_true', default=False )
    parser.add_argument( '--dryrun', help='do not upload data', default=False, action='store_true' )
    
    args = vars( parser.parse_args() )
    
    if args['verbose']:
        args['level'] = 'DEBUG'
    else:
        args['level'] = 'WARN'
    args['level'] = getattr( logging, args['level'].upper() )
    logging.basicConfig( **args )
    
    # set shared data output
    mgr = multiprocessing.Manager()
    data = mgr.dict()

    # configure jobs
    jobs = []
    
    # use input devices if requested (overwrite default full list)
    if len( args['device'] ) > 0 and 'driver' in args and args['driver']:
        mapping = {}
        for d in args['device']:
            mapping[d] = args['driver']

    for device, d in mapping.iteritems():

        logging.info("device: %s" % (device,))
        
        # normalise name
        device = device.lower()
        # setup driver
        driver = d.replace( '.', '_' )
        driver_klass = locals()[driver]
        
        # setup job
        jobs.append( multiprocessing.Process( target=worker, args=(device, driver_klass, data) )  )
        
    # run job
    for j in jobs:
        j.start()
        
    # await all finish
    for j in jobs:
        j.join()

    # send to influx
    # print data
    if args['dryrun']:
        print( dict(data) )
    else:
        if args['stdout']:
            print_as_influxdb( dict(data) )
        else:
            send_to_influxdb( dict(data) )
            
    sys.exit(0)
    
