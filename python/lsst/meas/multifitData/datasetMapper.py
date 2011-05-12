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
        file = "psf%(id)i.boost"%dataId
        path = os.path.join(self.root, file)
        return dafPer.ButlerLocation(\
                "lsst.afw.detection.Psf", 
                "Psf",
                "BoostStorage", path, {})

    def map_exp(self, dataId):        
        file = "exp%(id)i.fits"%dataId
        path = os.path.join(self.root, file)
        return dafPer.ButlerLocation(\
                "lsst.afw.image.ExposureD", 
                "ExposureD",
                "FitsStorage", path, {})
    
    def map_src(self, dataId):
        file = "src%(id)i.boost"%dataId
        path = os.path.join(self.root, file)        
        return dafPer.ButlerLocation(\
                "lsst.afw.detection.PersistableSourceVector", 
                "PersistableSourceVector",
                "BoostStorage", path, {"doFootprints": True})
       
    def std_src(self, item, dataId):
        return item.getSources()

    def keys(self):
        return ["id"]

 
