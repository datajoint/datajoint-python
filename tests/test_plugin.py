# import datajoint.errors as djerr
# import datajoint.plugin as p
# import importlib


# def test_normal_djerror():
#     try:
#         raise djerr.DataJointError
#     except djerr.DataJointError as e:
#         assert(e.__cause__ is None)


# def test_unverified_djerror():
#     try:
#         curr_plugins = p.discovered_plugins
#         p.discovered_plugins = dict(test_plugin_module=dict(verified=False, plugon='example'))
#         importlib.reload(djerr)
#         raise djerr.DataJointError
#     except djerr.DataJointError as e:
#         p.discovered_plugins = curr_plugins
#         importlib.reload(djerr)
#         assert(e.__cause__ is None)
#         # p.discovered_plugins = curr_plugins
#         # importlib.reload(djerr)
#         # print(isinstance(e.__cause__, djerr.PluginWarning))
#         # assert(isinstance(e.__cause__, djerr.PluginWarning))


# # def test_verified_djerror():
# #     assert_equal(get_host('hub://fakeservices.datajoint.io/datajoint/travis'),
# #         'fakeservices.datajoint.io:3306')
