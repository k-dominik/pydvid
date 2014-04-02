DVID Python Client
==================
Simple client for retrieving volume cutout data from a [DVID][] server as [vigranumpy][] arrays.

[DVID]: https://github.com/janelia-flyem/dvid
[vigranumpy]: http://ukoethe.github.io/vigra/doc/vigranumpy/index.html

VoxelsAccessor
------------
**Usage:**

```python
import numpy
from dvidclient.volume_client import VoxelsAccessor
from dvidclient.voxels_metadata import VoxelsMetadata
 
# Create a new remote volume
uuid = 'abcde'
voxels_metadata = VoxelsMetadata.create_default_metadata( (4,200,200,200), numpy.uint8, 'cxyz', 1.0, "" )
VoxelsAccessor.create_volume( "localhost:8000", uuid, "my_volume", voxels_metadata )

# Open connection for a particular volume    
vol_client = VoxelsAccessor( "localhost:8000", uuid, "my_volume" )
 
# Read from it
cutout_array = vol_client.retrieve_subvolume( (0,10,20,30), (4,110,120,130) ) # First axis is channel.
assert isinstance(cutout_array, numpy.ndarray)
assert cutout_array.shape == (4,100,100,100)

# Modify it
new_data = numpy.ones( (4,100,100,100), dtype=numpy.uint8 ) # Must include all channels.
cutout_array = vol_client.modify_subvolume( (0,10,20,30), (4,110,120,130), new_data )
```

**TODO**:
- Allow users to provide a pre-allocated array when requesting data
- Support multithreaded parallel requests
- Both client and server try to avoid using lots of memory by "chunking" data onto the http stream. Is that necessary?  Does it help?  Does it hurt?
- Resolution units are not used.
- Volume metainfo json is not validated against a schema

DVID Contents Browser UI
------------------------
A simple widget for viewing the list of datasets and nodes in a DVID instance is provided.
Requires PyQt4.  To see a demo of it in action, start up your dvid server run this:

    $ python dvidclient/gui/contents_browser.py localhost:8000

**TODO:**
- Show more details in dataset list (e.g. shape, axes, pixel type)
- Show more details in node list (e.g. date modified, parents, children)
- Gray-out nodes that aren't "open" for adding new volumes

Mock Server
-----------
For test purposes, a mock DVID server is implemented in the mockserver directory.
It serves up HDF5 datasets over http using the [DVID REST API][].

[DVID REST API]: http://godoc.org/github.com/janelia-flyem/dvid/datatype/voxels#pkg-constants

The mock server pulls its data from an hdf5 file with a special structure.
The `H5MockServerDataFile` utility class can be used to generate the file:

```python
import numpy
from mockserver.h5mockserver import H5MockServerDataFile

# Generate a volume to store.
data = numpy.random.randint( 0, 256, (1,100,200,300) )
voxels_metadata = VoxelsMetadata.create_default_metadata( (1,100,200,300), numpy.uint8, 'cxyz', 1.0, "" )

# Create special server datafile with one dataset, with one node.
# Then add our data volume to it.
with H5MockServerDataFile( 'mock_storage.h5' ) as server_datafile:
    server_datafile.add_node( 'my_dataset', 'abc123' )
    server_datafile.add_volume( 'my_dataset', 'my_volume', data_view, voxels_metadata )
```

Once you have a datafile, the server can be started from the command line:

    $ cd mockserver
    $ PYTHONPATH=.. python h5mockserver.py mock_storage.h5

See [`h5mockserver.py`][] the datafile format details.

[`h5mockserver.py`]: /mockserver/h5mockserver.py

Tests
-----
The unit tests require nosetests.

    $ cd tests
    $ PYTHONPATH=.. nosetests .
