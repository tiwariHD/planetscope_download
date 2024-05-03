import os
import shutil
import pprint
import requests
from requests.auth import HTTPBasicAuth
import subprocess
from time import time
from utils_file_info_excel import save_file_properties

# Enter planetscope api key here
API_KEY = ''


class Order:
    def __init__(self, base_dir, order_id_file, data_dirname, tmp_dirname):
        self.auth = HTTPBasicAuth(API_KEY, '')
        self.data_dir = os.path.join(base_dir, data_dirname)
        self.tmp_dir = os.path.join(base_dir, tmp_dirname)
        self.order_list = []
        with open(order_id_file, 'r') as file:
            for line in file:
                self.order_list.append(line.strip())
        assert len(self.order_list) > 0, "No Order ids present.. aborting"

    def check_order_status(self):
        all_downloadable = True
        for order_id in self.order_list:
            order_url = f"https://api.planet.com/compute/ops/orders/v2/{order_id}"
            response = requests.get(order_url, auth=self.auth)
            order_details = response.json()
            if order_details['state'] == 'success':
                print('Download ready: ', order_details['_links']['results'])
                order_details_filename = os.path.join(self.tmp_dir, 'order_details_' + order_id + '.txt')
                if not os.path.exists(order_details_filename):
                    with open(order_details_filename, 'w') as file:
                        pprint.pprint(order_details, stream=file)
            else:
                all_downloadable = False
                print('{}: {}'.format(order_id, order_details['last_message']))
        return all_downloadable

    def download_order(self):
        for order_id in self.order_list:
            order_url = f"https://api.planet.com/compute/ops/orders/v2/{order_id}"
            response = requests.get(order_url, auth=HTTPBasicAuth(API_KEY, ''))
            order_details = response.json()
            if order_details['state'] == 'success':
                order_dir = os.path.join(self.tmp_dir, order_id)
                if not os.path.exists(order_dir):
                    os.makedirs(order_dir)
                print('Downloading id: {} to: {}'.format(order_id, order_dir))
                results = order_details['_links']['results']
                for result in results:
                    subprocess.run(["wget", "-P", order_dir, "--content-disposition", result['location']], check=True)
            else:
                print('{}: {}'.format(order_id, order_details['last_message']))

    def unzip_and_move(self):
        for order_id in self.order_list:
            order_dir = os.path.join(self.tmp_dir, order_id)
            zip_file = os.path.join(order_dir, [file for file in os.listdir(order_dir) if file.endswith('zip')][0])
            print('Unzipping: {} in: {}'.format(zip_file, order_dir))
            subprocess.run(["unzip", "-d", order_dir, "-n", zip_file], stdout=subprocess.DEVNULL, check=True)

        for order_id in self.order_list:
            order_files_dir = os.path.join(self.tmp_dir, order_id, 'files')
            print('Moving files from: {} to: {}'.format(order_files_dir, self.data_dir))
            subprocess.run([f"mv {order_files_dir}/* {self.data_dir}"], stdout=subprocess.DEVNULL, shell=True, check=True)

    def clear_unzipped_order_dirs(self):
        for order_id in self.order_list:
            order_files_dir = os.path.join(self.tmp_dir, order_id, 'files')
            if os.path.exists(order_files_dir):
                print('Deleting: ', order_files_dir)
                shutil.rmtree(order_files_dir)


def main(base_dir, order_filepath, data_dirname='downloads', tmp_dirname='tmp'):
    orders = Order(base_dir, order_filepath, data_dirname, tmp_dirname)
    if not orders.check_order_status():
        print('Downloads not ready yet.. retry later..')
    else:
        try:
            orders.download_order()
            orders.unzip_and_move()
            print('Generating file info excel..')
            save_file_properties(
                os.path.join(base_dir, data_dirname),
                os.path.join(base_dir, 'downloaded_files_info.xlsx')
            )
        except Exception as e:
            print(f"Some error occurred: {e}")
        finally:
            orders.clear_unzipped_order_dirs()


if __name__ == '__main__':
    start = time()
    # replace dir paths
    dir_info = [
        {
            'base_dir': 'data',
            'order_filepath': 'order_ids.txt'
        }
    ]
    main(**dir_info[0])
    print('Run Process Time: {}'.format(time() - start))
