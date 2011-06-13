# -*- python -*-
#
# Setup our environment
#
import glob, os
import lsst.SConsUtils as scons

env = scons.makeEnv("meas_multifitData",
                    r"$HeadURL$",
                    [])


env['IgnoreFiles']=r"(~$|\.pyc$|^\.svn$|\.os$)"

Alias("install", [env.InstallEups(env['prefix'] + "/ups", glob.glob("ups/*.table")),
                   env.Install(env['prefix'], "python"),
                   env.Install(env['prefix'], "bin"),
                   env.Install(env['prefix'], "datasets")])

scons.CleanTree(r"*~ core *.so *.os *.o")

env.Declare()

env.Help("""
Reference data for lsst/meas/multifit
""")
