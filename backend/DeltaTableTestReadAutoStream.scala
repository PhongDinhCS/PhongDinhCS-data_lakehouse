// Define a function to read the Delta table and sleep for 5 seconds
def ReadDeltaFromHDFSloadAndSleep(): Unit = {
  ReadDeltaFromHDFS.main(Array())
  Thread.sleep(5000) // Sleep for 5 seconds
}

// Start the loop
while (true) {
  ReadDeltaFromHDFSloadAndSleep()
}