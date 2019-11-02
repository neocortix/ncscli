'''running inside blender, reconfigures compositor to take input from an exr image file'''
# standard library modules
import argparse
import sys
# third-party modules
import bpy

def quit( retCode, msg ):
    print( msg, file=sys.stderr )
    sys.exit( retCode )

print( 'reconfiguring compositor graph', file=sys.stderr )

ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
ap.add_argument( '--prerendered', default='prerendered.exr', help='name of prerendered intermediate file to read' )

argv = sys.argv
if "--" not in argv:
    argv = []  # as if no args are passed
else:
    argv = argv[argv.index("--") + 1:]  # get all args after "--"
args = ap.parse_args(argv)
#print('prerendered arg:', args.prerendered, file=sys.stderr)

scene=bpy.context.scene
scene.render.resolution_percentage=100

tree = scene.node_tree
#tree = bpy.data.scenes[0].node_tree
if not tree:
    quit( 90, 'the compositor tree is empty (or None)' )

fileName = args.prerendered
filePath = '//' + fileName
#filePath = 'G:\\Share\\Neo\\blendage\\examples\\bmw27\\prerendered.png'
# 'G:\\Share\\Neo\\blendage\\compositeTest\\cube.png'
# create a new image node, loading an image file
try:
    img = bpy.ops.image.open(filepath=filePath)
except RuntimeError:
    quit( 91, 'could not open prerendered image' )

try:
    imgNode = tree.nodes.new(type = 'CompositorNodeImage')
    try:
        imgNode.image = bpy.data.images[fileName]
    except KeyError:
        quit( 92, 'prerendered image not available' )
    imgNode.name = 'prerendered'
    scene.render.resolution_x = imgNode.image.size[0]
    scene.render.resolution_y = imgNode.image.size[1]

    # get the render layer node, originally use as input to compositor
    rlNode = tree.nodes['Render Layers']

    # swap in the image node, everywhere the "Render Layers" node was used
    for link in tree.links:
        if link.from_node == rlNode:
            toSocket = link.to_socket
            sockName = link.from_socket.name  # was toSocket.name
            tree.links.remove( link )  # hope this wont mess up the iteration
            fromSocket = imgNode.outputs[sockName]
            #print( 'would link', sockName )
            tree.links.new( fromSocket, toSocket )

    # remove the now-orphaned render layer node
    tree.nodes.remove( rlNode )
    print( 'reconfiguring compositor graph done', file=sys.stderr )
except Exception as exc:
    print( 'Exception reconfiguring compositor graph', type(exc), exc, file=sys.stderr )
    quit( 93, 'Exception reconfiguring compositor graph' )
