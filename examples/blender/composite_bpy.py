'''running inside blender, reconfigures compositor to take input from an exr image file'''
print( 'reconfiguring compositor graph' )
# standard library modules
import argparse
import sys
# third-party modules
import bpy

ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
ap.add_argument( '--prerendered', default='prerendered.exr', help='name of prerendered intermediate file to read' )

argv = sys.argv
if "--" not in argv:
    argv = []  # as if no args are passed
else:
    argv = argv[argv.index("--") + 1:]  # get all args after "--"
args = ap.parse_args(argv)
print('prerendered arg:', args.prerendered)

scene=bpy.context.scene
scene.render.resolution_percentage=100

#print( 'reconfiguring compositor graph' )

tree = scene.node_tree
#tree = bpy.data.scenes[0].node_tree

fileName = args.prerendered
filePath = '//' + fileName
#filePath = 'G:\\Share\\Neo\\blendage\\examples\\bmw27\\prerendered.png'
# 'G:\\Share\\Neo\\blendage\\compositeTest\\cube.png'
# create a new image node, loading an image file
try:
    img = bpy.ops.image.open(filepath=filePath)
except RuntimeError:
    print( 'could not open prerendered image')
imgNode = tree.nodes.new(type = 'CompositorNodeImage')
try:
    imgNode.image = bpy.data.images[fileName]
except KeyError:
    print( 'prerendered image not available')
imgNode.name = 'prerendered'

# get the render layer node, originally use as input to compositor
rlNode = tree.nodes['Render Layers']

# swap in the image node, everywhere the "Render Layers" node was used
for link in tree.links:
    if link.from_node == rlNode:
        toSocket = link.to_socket
        tree.links.remove( link )  # hope this wont mess up the iteration
        sockName = toSocket.name
        #print( 'would link', sockName )
        fromSocket = imgNode.outputs[sockName]
        tree.links.new( fromSocket, toSocket )

# remove the now-orphaned render layer node
tree.nodes.remove( rlNode )
print( 'reconfiguring compositor graph done' )
