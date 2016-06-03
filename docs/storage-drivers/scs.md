<!--[metadata]>
+++
title = "SCS storage driver"
description = "Explains how to use the Sohu Cloud Storage drivers"
keywords = ["registry, service, driver, images, storage,  scs, sohu, cloud"]
+++
<![end-metadata]-->


# Sohu Cloud Storage driver

An implementation of the `storagedriver.StorageDriver` interface which uses Sohu Cloud for object storage.

## Parameters


<table>
  <tr>
    <th>Parameter</th>
    <th>Required</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>
      <code>accesskey</code>
    </td>
    <td>
      yes
    </td>
    <td>
      Storage access key.
    </td>
  </tr>
  <tr>
    <td>
      <code>secretkey</code>
    </td>
    <td>
      yes
    </td>
    <td>
      Storage secret key.
    </td>
  </tr>
   <tr>
    <td>
      <code>region</code>
    </td>
    <td>
      yes
    </td>
    <td>
      Storage region.
    </td>
  </tr>
  <tr>
  	<td>
  	  <code>bucket</code>
  	</td>
  	<td>
  	  yes
  	</td>
  	<td>
  	  Storage bucket name.
    </td>
  </tr>

</table>


`accesskey`: The access key of your Sohu Cloud Storage account.

`secretkey`: The secret key of your Sohu Cloud Storage account.

`region`: The region of your Sohu Cloud Storage bucket (one of bjcnc/bjctc/shctc/gzctc).

`bucket`: The name of your Sohu Cloud Storage bucket where you wish to store objects (needs to already be created prior to driver initialization)..
