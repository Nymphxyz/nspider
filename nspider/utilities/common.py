#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @license: (C) Copyright 2020-2020
# @contact: xyz.hack666@gmail.com
# @file: kit.py
# @time: 2020.11.09 17:29
# @desc:

import os
import re
import pickle
import requests

# from nspider.core.request import Request

illegal_char_for_file = re.compile(r"[*?\"<>|/:]")

#编码对应文件后缀名
CODE_TO_EXTENSION = {
    "MP2T": "ts",
    "jpeg": "jpg",
    "M-JPEG": "avi",
}

def save_data(data, file_name):
    if(data):
        with open(file_name,'wb') as f:
            pickle.dump(data,f)

def load_data(file_name):
    try:
        with open(file_name,'rb') as cache_file:
            data = pickle.load(cache_file)
            return data
    except:
        return False

def get_size(obj, seen=None):
    import sys
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size

def valid_file_name(name):
    return re.sub(illegal_char_for_file, "", name)

def get_res_from_request(request):
    try:
        if request.method == "POST":
            handler = request.session.post
        else:
            handler = request.session.get
        res = handler(url=request.url, params=request.params, data=request.data, headers=request.headers,
                      cookies=request.cookies, proxies=request.proxies)
    except Exception as err:
        return None, err
    return res, None

def get_text_from_res(res):
    if isinstance(res, requests.models.Response):
        try:
            """
            因为返回的是string，这个string是根据return code编码的.
            所以先encode回byte，然后根据实际编码decode成string
            """
            html_text = res.text
            return_code = res.encoding  # 页面返回的编码方式
            actual_code = res.apparent_encoding  # 页面实际编码方式
            html_text = html_text.encode(return_code).decode(actual_code)
        #            print(html.encoding)
        #            print(html.apparent_encoding)
        # html_text = html_text.decode("utf-8")
        except Exception as err:
            # print("Error: ",url," ",format(err))
            html_text = res.text
            # return html_text
        return html_text
    raise ValueError("input param {} must be {}".format(res, requests.models.Response))


def if_blank_return(input, default) -> object:
    """
    :param default:
    :return: object
    if input value is none or blank element return default value
    [] "" are blank element
    """
    if not input:
        return default
    return input

def download(url, filename=None, save_dir=None, stream=True, chunk_size=1024*1024*2, verbose=False, try_num=2, fix_type=True, cookies=None,
                      headers=None, params=None, data=None, proxies=None, session=requests.session(), log = print, log_exception = print):
    """
    调用requests库的分流下载
    url 资源下载地址
    filename 保存文件名
    stream 参数表示分流下载
    verbose 表示显示下载进度
    try_num 失败后重新尝试下载次数
    fix_type 根据返回头自动修正文件名后缀
    cookies 自定义cookies
    headers 自定义请求头
    """
    if (verbose):
        log("Downloading {}... ".format(filename))

    if not filename: filename = os.path.basename(url)

    if not save_dir:
        filepath = filename
    else:
        filepath = os.path.join(save_dir, filename)

    for i in range(0, try_num):
        try:
            resource = session.get(url, stream=stream, headers=headers, cookies=cookies, params=params, data=data, proxies=proxies)

            if (resource.status_code == 200):
                is_chunked = resource.headers.get('transfer-encoding', '') == 'chunked'
                content_length_s = resource.headers.get('content-length')
                if not is_chunked and content_length_s.isdigit():
                    content_length = int(content_length_s)
                else:
                    content_length = None

                length = content_length if content_length else -1

                content_type = resource.headers.get('content-type')

                if content_type:
                    file_type = content_type.split("/")[-1]

                    extension = CODE_TO_EXTENSION.get(file_type)
                    if not extension: extension = file_type

                    paths = filename.split("/")
                    names = paths[-1].split(".")

                    if (fix_type and names[-1].upper() != extension.upper()):
                        if (len(names) == 1):
                            filename = filename + "." + extension
                        else:
                            names[-1] = extension
                            paths[-1] = ".".join(names)
                            filename = "/".join(paths)

                        if not save_dir:
                            filepath = filename
                        else:
                            filepath = os.path.join(save_dir, filename)

                dirname = os.path.dirname(filepath)
                if dirname and dirname != "./":
                    if not os.path.exists(dirname):
                        os.makedirs(dirname)

                try:
                    with open(filepath, mode="wb") as fh:
                        if (stream):
                            already = 0

                            for chunk in resource.iter_content(chunk_size=chunk_size):
                                already += len(chunk)
                                fh.write(chunk)
                                if (verbose):
                                    log("\r%d%% (%.2fMB/%.2fMB)" % (
                                    already / length * 100, already / (1024 * 1024), length / (1024 * 1024)), end='')
                        else:
                            fh.write(resource.content)
                except Exception as err:
                    if (os.path.exists(filepath)):
                        os.remove(filepath)
                    raise err
            else:
                log("Status code:{} Can't download resourse:{}".format(resource.status_code, url))
                return False

            if verbose:
                log(" √")
                log("Save as:", filepath)
            return True

        except Exception as err:
            if (i < try_num - 1):
                # print("Download Failed!: ",format(err),i+1," Retrying...")
                pass
            else:
                if verbose:
                    log(" ×")

                log("Download Failed! [url]:{} Caused by:".format(url))
                log_exception(err)
                return False

def str_2_dict(str, split=";"):
    dct = {}
    lst = str.split(split)
    for i in lst:
        name = i.split('=')[0].strip()
        value = i.split('=')[1].strip()
        dct[name] = value
    return dct