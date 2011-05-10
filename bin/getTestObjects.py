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

    if (len(sys.argv)>2):
        prefix = sys.argv[2]
    else: prefix = "."

    bf = ButlerFactory(mapper=DatasetMapper())
    outButler = bf.create()
    """
    try:
        os.makedirs(outPath)
    except os.error:
        raise "Conflict: requested output directory already exists."
    """
    if (len(sys.argv)>3):
        random.seed(int(sys.argv[3]))

    butlerPolicy = Policy()
    butlerPolicy.set("mapperName", "lsst.obs.lsstSim.LsstSimMapper")
    butlerPolicy.set("mapperPolicy.doFootprints", True)
    butlerPolicy.set("mapperPolicy.root", root)
    bf = ButlerFactory(butlerPolicy)
    butler = bf.create()
    """
    outputPolicy = Policy()
    outputPolicy.add("parameters.additionalData", "doFootprints=doFootprints")

    fmt = "parameters.outputItems.%s%i.storagePolicy"
    clipboard = {"doFootprints":True}
    """
    datasetId =0
    for (v,r,s) in butler.queryMetadata("raw", "sensor", ("visit", "raft", "sensor")):
        if butler.datasetExists("src", visit=v, raft=r, sensor=s) and \
               butler.datasetExists("psf", visit=v, raft=r, sensor=s) and \
               butler.datasetExists("calexp", visit=v, raft=r, sensor=s):

            exposure = butler.get("calexp", visit=v, raft=r, sensor=s)
            psf = butler.get("psf", visit=v, raft=r, sensor=s)
            sources = butler.get("src", visit=v, raft=r, sensor=s).getSources()
                
            #try:
            (lowPs, highPs, lowSg, highSg) = processDataset(exposure, psf, sources)
            #except:
            #    print >> sys.stderr, "Skiping dataset v=%i r=%s s=%s"%(v, r, s)
            #    continue
            
            outButler.put(psf, "psf", id=datasetId)
            for (src, dsType) in zip((lowPs, highPs, lowSg, highSg), ("lowPs", "highPs", "lowSg", "highSg")):
                if not src:
                    continue
                sub = makeCutout(src, exposure)
                psv = makePsv(src)
                outButler.put(sub, "exp",  id=datasetId, dsType=dsType)
                outButler.put(psv, "src",  id=datasetId, dsType=dsType)

            datasetId += 1

            """
            datasetPath = os.path.join(outPath, "%i"%datasetId)
            os.makedirs(datasetPath)
            addOutputPolicy(outputPolicy, datasetPath, datasetId)
            addClipboard(clipboard, datasetId, exposure, psf, lowPs, highPs, lowSg, highSg)



    outputStage = OutputStage(outputPolicy)
    sst = SimpleStageTester()
    sst.addStage(outputStage)
    sst.runWorker(clipboard)
    """

def makeCutout(src, exposure):
    bbox = src.getFootprint().getBBox()
    return exposure.Factory(exposure, bbox, afwImg.PARENT)

def makePsv(src):
    sourceSet = afwDet.SourceSet()
    sourceSet.append(src)
    return afwDet.PersistableSourceVector(sourceSet)

def addOutputPolicy(policy, outPath, datasetId):
    fmt = "parameters.outputItems.%s%s%i.storagePolicy"
    for srcType in ["lowPs", "highPs", "lowSg", "highSg"]:
        expLoc = os.path.join(outPath, srcType+"Exp.fits")
        srcLoc = os.path.join(outPath, srcType+"Src.boost")
        
        policy.add(fmt%(srcType, "Exp", datasetId) + ".storage", "FitsStorage")
        policy.add(fmt%(srcType, "Exp", datasetId) + ".location", expLoc)

        policy.add(fmt%(srcType, "Src", datasetId) + ".storage", "BoostStorage")
        policy.add(fmt%(srcType, "Src", datasetId) + ".location", srcLoc)

    psfLoc = os.path.join(outPath, "psf.boost")
    policy.add(fmt%("psf", "", datasetId) + ".storage", "BoostStorage")
    policy.add(fmt%("psf", "", datasetId) + ".location", psfLoc)

def addClipboard(clipboard, datasetId, exposure, psf, lowPs, highPs, lowSg, highSg):
    clipboard["lowPsExp%i"%datasetId]= makeCutout(lowPs, exposure)
    clipboard["highPsExp%i"%datasetId]= makeCutout(highPs, exposure)
    clipboard["lowSgExp%i"%datasetId]= makeCutout(lowSg, exposure)
    clipboard["highSgExp%i"%datasetId]= makeCutout(highSg, exposure)

    clipboard["psf%i"%datasetId]= psf

    clipboard["lowPsSrc%i"%datasetId]  = makePsv(lowPs)
    clipboard["highPsSrc%i"%datasetId] = makePsv(highPs)
    clipboard["lowSgSrc%i"%datasetId]  = makePsv(lowSg)
    clipboard["highSgSrc%i"%datasetId] = makePsv(highSg)


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
