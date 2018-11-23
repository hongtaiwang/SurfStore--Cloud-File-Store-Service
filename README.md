# This is a simple distributed file storage system based on RPC
# 

Use the following commands to run the blockstore, metadata store and the client (tap 'n' or 'h' to choose hash placement or nearest to client mode) - 

1. Blockstore - 

   ```shell
   python blockstore.py <port-number>
   ```

2. Metadata store - 

   ```shell
   python metastore.py config.txt
   ```

3. Client - 

   ```shell
   // to download a file
   python3 client.py mode config.txt download myfile.jpg folder_name/
   
   // to upload a file
   python3 client.py mode config.txt upload myfile.jpg
   
   // to delete a file
   python3 client.py mode config.txt delete myfile.jpg
   ```
