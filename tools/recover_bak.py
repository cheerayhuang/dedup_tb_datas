import os
import random
import re

import subprocess


start = 5099 
end = 5533 

# last file, 350

def append_to_target(src_files, target):
    p = subprocess.Popen(['cp', target, target+'.bak', '-f'], shell=False)
    print(f'backup {target}: {p.wait()}')

    f_target = open(target, 'a')

    count = 0
    for src in src_files: 
        print(f'reading {src} ...')
        f_src = open(src, 'r')
        lines = f_src.readlines()
        f_src.close()

        for i, l in enumerate(lines):
            if len(l) < 400: 
                lines[i] = ''
                continue

            r1 = random.random() * 10 + 25
            r2 = random.random() * 100

            if r2 > r1:
                lines[i] = ''

        f_target.writelines(lines)

        p = subprocess.Popen(['mv', src, src+'.bak'], shell=False)
        print(f'backup {src}: {p.wait()}')

        p = subprocess.Popen(['touch', src], shell=False)
        print(f'zero {src}: {p.wait()}')
        count += 1
        print(f'--- {count} ---')
        
    f_target.close()


def main():
    for root, dirs, files in os.walk('./data'):
        target_file = ''
        for f in files:
            if f.endswith('jsonl.bak'):
                target_file = root + '/' + f
                p = subprocess.Popen(['mv', target_file, target_file.rstrip('.bak')]) 
                print(f'recover {f}: {p.wait()}')

                    

if __name__ == '__main__':
    main()
