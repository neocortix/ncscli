import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import math
import numpy as np
import sys
# print ('Number of arguments: %d arguments.' % len(sys.argv))
# print ('Argument List: %s' % str(sys.argv))
if len(sys.argv)>1:
  frameNum = int(sys.argv[1])
else:
  frameNum = 1
print("Starting plot_sine_001.py, frameNum = %d" % frameNum)
myDPI = 300
numPoints = 2000
numcycles = frameNum
x = np.zeros([numPoints+1])
for i in range(0,numPoints+1):
    x[i] = i
y = np.sin(numcycles*x*2*math.pi/numPoints)

fig = plt.figure(3,figsize=(20.11,2),dpi=myDPI) # empirically leads to 1920x1080 output
plt.plot(x,y,color='black',linewidth=4)
plt.axis('off')
plt.fill_between(x, y, where=y>=0, interpolate=True, color='blue',alpha=0.5)
plt.fill_between(x, y, where=y<=0, interpolate=True, color='red',alpha=0.5)
plt.show()
fileName = "./sine_%06d.png" % frameNum
print("Writing file:  %s" % fileName)
fig.tight_layout()
fig.savefig(fileName, bbox_inches='tight' )
plt.close()
