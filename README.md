# planetscope_download
Python scripts for downloading Planetscope scenes

* Order is placed using REST calls to planet api.
Run all the cells in the jupyter notebook to place order.
Modify scene parameters as required.

* Download script runs at the moment only on unix shells, uses subprocess to call wget, etc.
To achieve the same in windows, modify the script to print the URL of the order.
And then copy the URL and download using wget, curl, etc or just paste in browser.

* Additional script applies [udm 2 masks](https://developers.planet.com/docs/data/udm-2/#udm21-bands) on the scenes.
And also expands each scene image to the extent of the biggest scene.
