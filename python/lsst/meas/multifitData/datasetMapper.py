import lsst.daf.persistence as dafPer
import lsst.afw.image
import lsst.afw.detection
import lsst.meas.algorithms

import eups
import os

class DatasetMapper(dafPer.Mapper):
    def __init__(self):
        productDir = eups.productDir("meas_multifitData")
        if not productDir:
            self.root = "datasets"
        else:
            self.root = os.path.join(productDir, "datasets")

    def map_psf(self, dataId):
        dir = "%(id)i"%dataId
        path = os.path.join(self.root, dir, "psf.boost")
        return dafPer.ButlerLocation(\
                "lsst.afw.detection.Psf", 
                "Psf",
                "BoostStorage", path, {})

    def map_exp(self, dataId):        
        dir = "%(id)i"%dataId
        file = "%(dsType)sExp.fits"%dataId
        path = os.path.join(self.root, dir, file)
        return dafPer.ButlerLocation(\
                "lsst.afw.image.ExposureD", 
                "ExposureD",
                "FitsStorage", path, {})
    
    def map_src(self, dataId):
        dir = "%(id)i"%dataId
        file = "%(dsType)sSrc.boost"%dataId
        path = os.path.join(self.root, dir, file)        
        return dafPer.ButlerLocation(\
                "lsst.afw.detection.PersistableSourceVector", 
                "PersistableSourceVector",
                "BoostStorage", path, {"doFootprints": True})
       
    def std_src(self, item, dataId):
        return item.getSources()[0]

    def keys(self):
        return ["id", "dsType"]
