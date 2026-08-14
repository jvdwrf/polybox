# \DefaultApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_tree**](DefaultApi.md#get_tree) | **GET** /tree | 



## get_tree

> models::SupervisionTree get_tree(include_debug, pid)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**include_debug** | Option<**bool**> | Whether to include debug state in the supervision tree |  |
**pid** | Option<**String**> | The PID from which to start the supervision tree. If not provided, the root supervisor will be used. |  |

### Return type

[**models::SupervisionTree**](SupervisionTree.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json, text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

