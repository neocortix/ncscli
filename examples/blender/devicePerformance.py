#!/usr/bin/env python3
"""
NCS device performance metrics
"""
# standard library modules
#none

def getColumn(inputList,column):
    return [inputList[i][column] for i in range(0,len(inputList))]

def getIndices(inputList,value):
    return [i for i, x in enumerate(inputList) if x == value]

cpuRatings = [
              ['Atom-Silvermont',3.0,'x86_64'],        # guess, better than A7 at higher power consumption
              ['Atom-Medfield',2.4,'x86'],        # guess, better than A7 at higher power consumption, but only used in 32-bit mode
              ['Atom-Midview',2.4,'x86'],        # guess, better than A7 at higher power consumption, but only used in 32-bit mode
              ['Cortex-A12',1.5],  # between A9 and A15 in performance, same performance as A17
              ['Cortex-A15',1.75], # high performance 32-bit, 40% better than A9
              ['Cortex-A35',1.4,'aarch64'],  # 40% better than A7
              ['Cortex-A35',1.1, 'armv7l'],  # 40% better than A7, but only used in 32-bit mode
              ['Cortex-A5',1.0],   # similar to A7
              ['Cortex-A53',1.25,'aarch64'], # 64-bit successor to A7
              ['Cortex-A53',1.0,  'armv7l'], # 64-bit successor to A7, but only used in 32-bit mode
              ['Cortex-A55',1.475],  # 18% better than A53
              ['Cortex-A57',3.75,'aarch64'], # 64-bit high performance, successor to A15  
              ['Cortex-A57',3.0, 'armv7l'], # 64-bit high performance, successor to A15, but only used in 32-bit mode  
              ['Cortex-A7',1.0],  # nominal reference processor, 32-bit, 50% better than A8
              ['Cortex-A72',1.5], # better than A53, not clear by how much, guess 1.25X better
              ['Cortex-A73',3.25], # 64-bit, used in Galaxy Note 8 in top 4 cores.  guess same as Exynos-M2
              ['Cortex-A75',3.75], # replaces A73 (only 1.5 if not aarch64)
              ['Cortex-A76',5.0], # from Dmitry's table (only 1.5 if not aarch64)
              ['Cortex-A9',1.25],  # same capability of A53 but lower power
              ['Denver',2.0],      # guess, 2014 technology from Nvidia
              ['Exynos-M1',2.5],   # guess similar to A9
              ['Exynos-M2',3.25],   # guess similar to A53
              ['Exynos-M3',3.75],   # guess similar to A57
              ['Exynos-M4',3.75],   # from Dmitry's table
              ['Krait',1.25],        # successor to Scorpion.  Guess similar to A9
              ['Kryo',3.4],          # 64-bit successor to Krait.  Similar to A53, good FP (was 1.25, created problem for domestic GS7)
              ['Scorpion',1.0]]     # early Qualcomm, pre-2012.  Guess similar to A7 

def ComputeDevicePerformanceRating(cpuarch,cpunumcores,cpuspeeds,cpufamily):
    # cpuarch:      string like "aarch64" or "armv7l"
    # cpunumcores:  int
    # cpuspeeds:    list of floats of length cpunumcores, each representing a clock frequency in GHz
    # cpufamily:    list of strings of length cpunumcores
    # returns devicePerformanceRating:  float
    for j in range(0,len(cpufamily)):
        cpufamily[j] = cpufamily[j].replace("Atom Midview","Atom-Midview").replace("Atom Medfield","Atom-Medfield").replace("Atom Silvermont","Atom-Silvermont")       
    devicePerformanceRating = 0
    for j in range(0,len(cpufamily)):
        rating = 0  
        family = cpufamily[j]
        indices = getIndices(getColumn(cpuRatings,0),family)
        if (len(indices)==1):
            rating = cpuRatings[getColumn(cpuRatings,0).index(family)][1]
        else:
            arch = cpuarch
            for k in range(0,len(indices)):           
                if (arch==cpuRatings[indices[k]][2]):
                    rating = cpuRatings[indices[k]][1]
        speed = cpuspeeds[j]
        devicePerformanceRating += rating*speed
    return devicePerformanceRating

devicePerformanceRating = ComputeDevicePerformanceRating  # an alias for different naming convention

if __name__ == "__main__":
    print( "this is a module, not a script" )
    # print the names of all the available identifiers
    print( [s for s in dir() if s[0] is not '_'] )
