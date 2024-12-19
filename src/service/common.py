from model import common as model
from data import common as data

async def createSpatialUnit(spatialUnit:model.CreateSpatialUnit):
    try:
        spatialUnit = model.spatialUnit(
            name = spatialUnit.name,
            type = spatialUnit.type,
            description = spatialUnit.description,
            unit_guide = spatialUnit.unitGuide
            )
        spatialUnitId = await data.createSpatialUnit(spatialUnit)
        return spatialUnitId
    except Exception as e:
        print(e)