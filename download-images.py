import sys, os, multiprocessing, csv
from urllib import request, error
from PIL import Image
from io import BytesIO


def parse_data(data_file):
    csvfile = open(data_file, 'r')
    csvreader = csv.reader(csvfile)
    key_url_list = [line[:3] for line in csvreader]
    return key_url_list[1:]  # Chop off header


def download_image(key_url):
    
    out_dir = "temp"
    (key, url, label_dir) = key_url

    label_dir_path = os.path.join(label_dir)

    if os.path.exists(label_dir_path):
        out_dir = label_dir
    else:
        os.mkdir(label_dir)
        out_dir = label_dir

    filename = os.path.join(out_dir, '{}.jpg'.format(key))

    if os.path.exists(filename):
        print('Image {} already exists. Skipping download.'.format(filename))
        return

    try:
        response = request.urlopen(url)
        image_data = response.read()
    except:
        print('Warning: Could not download image {} from {}'.format(key, url))
        return

    try:
        pil_image = Image.open(BytesIO(image_data))
    except:
        print('Warning: Failed to parse image {}'.format(key))
        return

    try:
        pil_image_rgb = pil_image.convert('RGB')
    except:
        print('Warning: Failed to convert image {} to RGB'.format(key))
        return

    try:
        pil_image_rgb.save(filename, format='JPEG', quality=90)
    except:
        print('Warning: Failed to save image {}'.format(filename))
        return


def loader():
    if len(sys.argv) != 2:
        print('Syntax: {} <data_file.csv>'.format(sys.argv[0]))
        sys.exit(0)
    (data_file, out_dir) = (sys.argv[1], "temp")
    
    key_url_list = parse_data(data_file)
    (key, url, label_dir) = key_url_list[0]

    if not os.path.exists(label_dir):
        os.mkdir(label_dir)

    pool = multiprocessing.Pool(processes=4)  # Num of CPUs
    pool.map(download_image, key_url_list)
    pool.close()
    pool.terminate()


# arg1 : data_file.csv
# arg2 : output_dir
if __name__ == '__main__':
    loader()
