# import sys
# sys.path.append('/usr/local/lib64/')

# import site; print(site.getsitepackages())

# import hsi
# help(hsi)

# from hsi import ios_base
# print (ios_base)

from hsi import *         # load the module
# help(Panorama)
pano=Panorama()              # make a new Panorama object
ifs=ifstream('t.pto')    # create a C++ std::ifstream
pano.readData(ifs)           # read the pto file into the Panorama object
del ifs                   # don't need anymore
# img0=pano.getImage(0)        # access the first image
# print (img0.getWidth())     # print the image's width
# cpv=pano.getCtrlPoints()     # get the control points in the panorama
# for cp in cpv[:30:2] :    # print some data from some of the CPs
#   print (cp.x1)
# cpv=cpv[30:50]            # throw away most of the CPs
# pano.setCtrlPoints(cpv)      # pass that subset back to the panorama
# ofs=ofstream('yy.pto')    # make a c++ std::ofstream to write to
# pano.writeData(ofs)          # write the modified panorama to that stream
# del ofs                   # done with it

pano.setImageFilename(0,'image0.jpg')
pano.setImageFilename(1,'image2.jpg')

opts = pano.getOptions()

opts.outputFormat = PanoramaOptions.TIFF_m
opts.outputImageType = "tif"
outputImages = getImagesinROI(pano, pano.getActiveImages())
exposureLayers = getExposureLayers(pano, outputImages, opts)

print(isinstance(exposureLayers, tuple))
print(exposureLayers)
print(exposureLayers.size())
if exposureLayers.empty():            
	print("ERROR: Could not determine exposure layers. Cancel execution.")
else:
	# HuginBase::Nona::SetAdvancedOption(advOptions, "basename", basename);
	for i in range(len(exposureLayers)):
	
		modOptions = PanoramaOptions(opts)
		# // set output exposure to exposure value of first image of layers
		# // normally this this invoked with --ignore-exposure, so this has no effect
		modOptions.outputExposureValue = pano.getImage(*(exposureLayers[i].begin())).getExposureValue();
		# // build filename
		# std::ostringstream filename;
		# filename << basename << std::setfill('0') << std::setw(4) << i;
		# HuginBase::NonaFileOutputStitcher(pano, pdisp, modOptions, exposureLayers[i], filename.str(), advOptions).run();
	
            