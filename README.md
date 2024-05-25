# Himawari

A basic python script for fetching all Himawari satellite images in a date range.

## Requirements:

+ [Python 3.10+](https://www.python.org/downloads/)
+ [Miniconda](https://docs.conda.io/en/latest/miniconda.html)

## Usage
### Installation
1. Download or clone the repository:
    ```bash
    git clone https://github.com/liquidstereo/himawari
    cd himawari
    ```

2. Create a new conda environment with python version.
    ```
    conda create -n Himawari python=3.10 -y
    conda activate Himawari
    ```
3. Dependencies can be installed by running the following command in your terminal:
    ```
    pip install -r requirements.txt
    ```

### Running the code
1. You can run this script simply by:

    ```
    python himawari.py -s 'YYYY-MM-DD hh:mm' -e 'YYYY-MM-DD hh:mm' -lv LEVEL -p [optional]
    ```
2.  Example usage to fetching images:

    ```
    python himawari.py -s '2024-05-24 00:00' -e '2024-05-24 23:50' -lv 2 -p
    ```
3. Command line arguments:

    ```
    Usage: python himawari.py [Arguments]

        -s, --start_time     Beginning date of downloaded files ('YYYY-MM-DD hh:mm')
        -e, --end_time       Ending date of downloaded files ('YYYY-MM-DD hh:mm')
        -lv, --level         Number of level (Level can be 1, 2, 4, 8, 16, or 20)
        -p, --preview        Preview result (default: false)

    ```

+ The time interval is available in 10 minute increments starting at 00:00
+ Create an image from 550px to 11000px based on the set level value.
+ 'No-images error / 403 Forbidden' might be produced due to routine satellite or server activity.

## Resources:
+ [Himawari-8 Real-time Web](http://himawari8.nict.go.jp/)
+ [Japan Meteorological Agency](http://www.jma.go.jp/)
+ [HimawariCast](https://www.data.jma.go.jp/mscweb/en/himawari89/himawari_cast/himawari_cast.php)
+ [Himawari-8/9 Standard Data User's Guide](http://www.data.jma.go.jp/mscweb/en/himawari89/space_segment/hsd_sample/HS_D_users_guide_en_v12.pdf)
+ [Table 3 Time interval of Himawari-8 observations and data files provided by JMA](https://link.springer.com/article/10.1007/s12145-017-0316-4/tables/3)
## License
+ All usage of True Color Reproduction (TCR) imagery provided here is subject to the Terms of Use for the [MSC/JMA](https://www.data.jma.go.jp/mscweb/en/general/note.html) website.
+ This project is licensed under the MIT License.