# D4SU Server

## Project Summary

This project aims to exemplify a reliable and maintainable server solution to bridge the gap between the AEC industry digital deliverables (such as IFC files) and the administative processess in the domains of Digital Building Permits, Energy Performance of Building Certification, Life Cycle Assessement, 3D Cadastre, ...  and support the needs of the D4SU project as documented in [d4SU-documentation](https://lvanlier.github.io).

<br>
<br>

<img src='assets/d4su.svg'>

The platform lets import IFC files, validate them against requirements defined in IDS and process their data (objects, relationships, properties and geometries). It leverages OpenBIM tools such as IfcOpenShell and provides APIs for business applications to easily consume this technical data. The data is made available in a PostgreSQL database and in JSON, IFC and CSV files depending on the needs.

The documentation is available on <https://lvanlier.github.io/>

The FastAPi app code is under

+ src/web for the web layer
+ src/service for the service layer
+ src/model for the model layer
+ src/data for the data layer

The Celery app code is under

+ src/long_bg_task
+ src/long_bg_task/task_modules (the 'core features of d4SU')
