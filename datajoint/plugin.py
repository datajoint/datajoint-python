import os
import pkg_resources
import hashlib
import base64
from pathlib import Path
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes

DJ_PUB_KEY = '''
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDUMOo2U7YQ1uOrKU/IreM3AQP2
AXJC3au+S9W+dilxHcJ3e98bRVqrFeOofcGeRPoNc38fiLmLDUiBskJeVrpm29Wo
AkH6yhZWk1o8NvGMhK4DLsJYlsH6tZuOx9NITKzJuOOH6X1I5Ucs7NOSKnmu7g5g
WTT5kCgF5QAe5JN8WQIDAQAB
-----END PUBLIC KEY-----
'''

discovered_plugins = {
    entry_point.module_name: dict(plugon=entry_point.name, verified=False)
    for entry_point
    in pkg_resources.iter_entry_points('datajoint.plugins')
}


def hash_pkg(pkgpath):
    refpath = Path(pkgpath).absolute().parents[0]
    details = ''
    details = _update_details_dir(pkgpath, refpath, details)
    # hash output to prepare for signing
    return hashlib.sha1('blob {}\0{}'.format(len(details), details).encode()).hexdigest()


def _update_details_dir(dirpath, refpath, details):
    paths = sorted(Path(dirpath).absolute().glob('*'))
    # walk a directory to collect info
    for path in paths:
        if 'pycache' not in str(path):
            if os.path.isdir(str(path)):
                details = _update_details_dir(path, refpath, details)
            else:
                details = _update_details_file(path, refpath, details)
    return details


def _update_details_file(filepath, refpath, details):
    if '.sig' not in str(filepath):
        with open(str(filepath), 'r') as f:
            data = f.read()
        # perfrom a SHA1 hash (same as git) that closely matches: git ls-files -s <dirname>
        mode = 100644
        hash = hashlib.sha1('blob {}\0{}'.format(len(data),data).encode()).hexdigest()
        stage_no = 0
        relative_path = str(filepath.relative_to(refpath))
        details = '{}{} {} {}\t{}\n'.format(details, mode, hash, stage_no, relative_path)
    return details


def _update_error_stack(module):
    try:
        pkg = pkg_resources.get_distribution(module.__name__)
        signature = pkg.get_metadata('datajoint.sig')
        pub_key = load_pem_public_key(bytes(DJ_PUB_KEY, 'UTF-8'), backend=default_backend())
        data = hash_pkg(module.__path__[0])
        pub_key.verify(
            base64.b64decode(signature.encode()),
            data.encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256())
        discovered_plugins[module.__name__]['verified'] = True
        print('DataJoint verified plugin `{}` introduced.'.format(module.__name__))
    except (FileNotFoundError, InvalidSignature):
        print('Unverified plugin `{}` introduced.'.format(module.__name__))


def override(plugin_type, context, method_list=None):
    relevant_plugins = {
        k: v for k, v in discovered_plugins.items() if v['plugon'] == plugin_type}
    if relevant_plugins:
        for module_name in relevant_plugins:
            # import plugin
            module = __import__(module_name)
            module_dict = module.__dict__
            # update error stack (if applicable)
            _update_error_stack(module)
            # override based on plugon preference
            if method_list is not None:
                new_methods = []
                for v in method_list:
                    try:
                        new_methods.append(getattr(module, v))
                    except AttributeError:
                        pass
                context.update(dict(zip(method_list, new_methods)))
            else:
                try:
                    new_methods = module.__all__
                except AttributeError:
                    new_methods = [name for name in module_dict if not name.startswith('_')]
                context.update({name: module_dict[name] for name in new_methods})
