from lsst.daf.persistence import ButlerFactory
from lsst.obs.lsstSim import LsstSimMapper
from datasetMapper import DatasetMapper
from lsst.pex.policy import Policy
import lsst.meas.algorithms as measAlg
import lsst.afw.image as afwImg
import lsst.afw.geom as afwGeom
import lsst.afw.math as afwMath
import lsst.afw.detection as afwDet
import lsst.afw.geom.ellipses
from lsst.pex.harness.IOStage import OutputStage
from lsst.pex.harness.simpleStageTester import SimpleStageTester
from lsst.meas.multifitData import DatasetMapper

import os
import os.path
import sys
import random
import math

def run():
    if (len(sys.argv)>1):
        root = sys.argv[1]
    else:
        raise "Need a butler root"

    bf = ButlerFactory(mapper=DatasetMapper())
    outButler = bf.create()

    if (len(sys.argv)>2):
        nOutput = int(sys.argv[2])
    else:
        nOutput = -1

    butlerPolicy = Policy()
    butlerPolicy.set("mapperName", "lsst.obs.lsstSim.LsstSimMapper")
    butlerPolicy.set("mapperPolicy.doFootprints", True)
    butlerPolicy.set("mapperPolicy.root", root)
    bf = ButlerFactory(butlerPolicy)
    butler = bf.create()
    
    query =  butler.queryMetadata("calexp", "sensor", ("visit", "raft", "sensor"))
    if(nOutput<0):
        nOutput = len(query)

    i = 0
    datasetId = 0
    while True:
        if(i >= len(query)):
            break
        (v,r,s) = query[i]
        i += 1
        if butler.datasetExists("src", visit=v, raft=r, sensor=s) and \
                butler.datasetExists("psf", visit=v, raft=r, sensor=s) and \
                butler.datasetExists("calexp", visit=v, raft=r, sensor=s):
            psv  = butler.get("src", visit=v, raft=r, sensor=s)
            if len(psv.getSources()) > 0:
                print "datasetId", datasetId
                print "i", i
                exposure = butler.get("calexp", visit=v, raft=r, sensor=s)
                psf = butler.get("psf", visit=v, raft=r, sensor=s)

                outButler.put(psf, "psf", id=datasetId)
                outButler.put(exposure, "exp", id=datasetId)
                outButler.put(psv, "src", id=datasetId)
                datasetId += 1
                if(datasetId >= nOutput):
                    break;



def makeCutout(src, exposure):
    bbox = src.getFootprint().getBBox()
    return exposure.Factory(exposure, bbox, afwImg.PARENT)

def makePsv(src):
    sourceSet = afwDet.SourceSet()
    sourceSet.append(src)
    return afwDet.PersistableSourceVector(sourceSet)

def processDataset(exposure, psf, sources):
    nLowPs = 0
    nHighPs = 0
    nLowSg = 0
    nHighSg = 0    
     
    mi = exposure.getMaskedImage()
    n=0

    lowPs=None
    highPs=None
    lowSg=None
    highSg=None
    print len(sources)
    while (nLowPs < 1 or nHighPs < 1 or nLowSg < 1 or nHighSg < 1) and n < len(sources):
        print "n=", n
        src = sources[n]

        n+=1

        print nLowPs
        print nHighPs
        print nLowSg
        print nHighSg


        if src.getFlagForDetection() & \
                (measAlg.Flags.INTERP_CENTER | measAlg.Flags.SATUR_CENTER):
            continue
        
        fp = src.getFootprint()
        if(fp.getArea() < 10 or fp.getArea() > 500):
            #deal only with midrange objects for now
            continue
        
        yc = src.getYAstrom()
        xc = src.getXAstrom()      
        
        psfAttributes = measAlg.PsfAttributes(psf, int(xc), int(yc))
        psfWidth = psfAttributes.computeGaussianWidth()
        #print "psf width", psfWidth
        
        quad = afwGeom.ellipses.Quadrupole(src.getIxx(), src.getIyy(), src.getIxy())
        srcRadius = quad.getTraceRadius()
        #print "src radius", srcRadius
       
        ps = False
        if srcRadius <= psfWidth:
            ps = True
        elif srcRadius >= 2*psfWidth:
            ps = False
        else:
            continue
       
        bbox = fp.getBBox()
        sub = mi.Factory(mi, bbox, afwImg.PARENT)
        
        bitmask=mi.getMask().getPlaneBitMask("DETECTED")
        ctrl=afwMath.StatisticsControl()
        ctrl.setAndMask(~bitmask)
        #ctrl.setWeighted(True)
        stats = afwMath.makeStatistics(sub, afwMath.SUM| afwMath.VARIANCE, ctrl)
        sumVal = stats.getValue(afwMath.SUM)
        varVal = stats.getValue(afwMath.VARIANCE)
        snr = sumVal/varVal
        #print "SNR", sn
        low = False
        if snr < 1:
            low = True
        elif snr > 5:
            low = False
        else:
            continue

        if low:
            if ps:
                nLowPs+=1
                lowPs = src
            else:
                nLowSg+=1
                lowSg = src
        elif ps:
            nHighPs+=1
            highPs = src
        else: 
            nHighSg+=1
            highSg = src
    #print >> sys.stderr, "n=",n

    return lowPs, highPs, lowSg, highSg

if __name__ == '__main__':
    run()
