# IFCJSON_python - ifc2json4.py
# Convert IFC SPF file to ifcJSON-4
# https://github.com/IFCJSON-Team

# MIT License

# Copyright (c) 2020 Jan Brouwer <jan@brewsky.nl>

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import uuid
import ifcopenshell
import ifcopenshell.geom
import ifcopenshell.guid as guid
# import ifcjson.common as common
from ..ifcjson import common as common
from datetime import datetime
from ifcopenshell.entity_instance import entity_instance


class IFC2JSON4(common.IFC2JSON):
    SCHEMA_VERSION = '0.0.1'

    settings = ifcopenshell.geom.settings()
    settings.set(settings.USE_WORLD_COORDS, False)

    def __init__(self,
                 ifcModel,
                 COMPACT=False,
                 NO_INVERSE=False,
                 EMPTY_PROPERTIES=False,
                 NO_OWNERHISTORY=False,
                 GEOMETRY=True,
                 STYLED=False):
        """IFC SPF to ifcJSON-4 writer

        parameters:
        ifcModel: IFC filePath or ifcopenshell model instance
        COMPACT (boolean): if True then pretty print is turned off and references are created without informative "type" property
        NO_INVERSE (boolean): if True then inverse relationships will be explicitly added to entities

        """

        self.COMPACT = COMPACT
        self.NO_INVERSE = NO_INVERSE
        self.EMPTY_PROPERTIES = EMPTY_PROPERTIES
        self.STYLED = STYLED

        if isinstance(ifcModel, ifcopenshell.file):
            self.ifcModel = ifcModel
        else:
            self.ifcModel = ifcopenshell.open(ifcModel)

        # Dictionary referencing all objects with a GlobalId that are already created
        self.rootObjects = {}
        self.styledMap = {}

        # input(dir(self.ifcModel.wrapped_data.header))
        # input(self.ifcModel.wrapped_data.header)
        # print(dir(self.ifcModel.wrapped_data.header.file_description))
        # if self.ifcModel.wrapped_data.header.file_description.this:
        #     print(self.ifcModel.wrapped_data.header.file_description[0])
        # input()
        
        if NO_OWNERHISTORY:
            self.remove_ownerhistory()

        # adjust GEOMETRY type
        if GEOMETRY == 'tessellate':
            print('<<<<<<<< Tessellating geometry >>>>>>>>>')
            self.tessellate()
        elif GEOMETRY == False:
            self.remove_geometry()

    def spf2Json(self):
        """
        Create json dictionary structure for all attributes of the objects in the root list
        also including inverse attributes (except for IfcGeometricRepresentationContext and IfcOwnerHistory types)
        # (?) Check every IFC object to see if it is used multiple times

        Returns:
        dict: ifcJSON-4 model structure

        """

        jsonObjects = []
        relationships = []

        # Collect all entity types that already have a GlobalId
        for entity in self.ifcModel.by_type('IfcRoot'):
            key = entity.id() # added LVL 2025-06-12
            if entity.is_a('IfcRelationship'):
                relationships.append(entity)
            else:
                self.rootObjects[entity.id()] = guid.split(
                    guid.expand(entity.GlobalId))[1:-1]
                self.styledMap[str(key)]=self.rootObjects[entity.id()]  # added LVL 2025-06-12

        # seperately collect all entity types where a GlobalId needs to be added
        # for entity in self.ifcModel.by_type('IfcMaterialDefinition'):
        #     self.rootObjects[entity.id()] = str(uuid.uuid4())

        # LVL 2025-06-12 changed this
        typesToAdd_List = [
            'IfcShapeRepresentation',
            'IfcOwnerHistory',
            'IfcGeometricRepresentationContext',
        ]
        if self.STYLED:
            styled_List = [
                'IfcBooleanClippingResult',
                'IfcBoundingBox',
                'IfcExtrudedAreaSolid',
                'IfcFaceBasedSurfaceModel',
                'IfcFacetedBrep',
                'IfcGeometricCurveSet',
                'IfcGeometricSet',
                'IfcIndexedPolyCurve',
                'IfcMappedItem',
                'IfcPolygonalFaceSet',
                'IfcStyledItem',
                'IfcPresentationStyleAssignment',
                'IfcPresentationLayerAssignment',
                'IfcSurfaceStyle',
                'IfcSurfaceStyleRendering',
                'IfcColourRgb',
                'IfcMaterialDefinitionRepresentation',
                'IfcStyledRepresentation'
                ]
            typesToAdd_List.extend(styled_List)
    
        for item in typesToAdd_List:
            try:
                for entity in self.ifcModel.by_type(item):
                    self.rootObjects[entity.id()] = str(uuid.uuid4())
                    self.styledMap[str(entity.id())]=self.rootObjects[entity.id()]
            except Exception as e:
                continue # some of the item are not present for IFC2X3
                
#
#        for key, value in self.styledMap.items():
#            print(f'key: {key}, value: {value}')
#
            
        # Seperately add all IfcRelationship entities so they appear at the end of the list
        for entity in relationships:
            self.rootObjects[entity.id()] = guid.split(
                guid.expand(entity.GlobalId))[1:-1]

        for key in self.rootObjects:
            entity = self.ifcModel.by_id(key)
            entityAttributes = entity.__dict__
            entityType = entityAttributes['type']
            if not entityType == 'IfcOwnerHistory':
                if not self.NO_INVERSE:
                    for attr in entity.wrapped_data.get_inverse_attribute_names():
                        inverseAttribute = getattr(entity, attr)
                        attrValue = self.getAttributeValue(inverseAttribute)
                        if not attrValue and attrValue is not False:
                            continue
                        else:
                            entityAttributes[attr] = attrValue

            entityAttributes["GlobalId"] = self.rootObjects[entity.id()]
            jsonObjects.append(self.createFullObject(entityAttributes))

        return {
            'type': 'ifcJSON',
            'version': self.SCHEMA_VERSION,
            # 'schemaIdentifiers': self.ifcModel.wrapped_data.header.file_schema.schema_identifiers,
            'schemaIdentifier': self.ifcModel.wrapped_data.schema,
            'originatingSystem': 'IFC2JSON_python Version ' + self.VERSION,
            'preprocessorVersion': 'IfcOpenShell ' + ifcopenshell.version,
            'timeStamp': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'data': jsonObjects
        }
        
    def createFullObject(self, entityAttributes):
        """Returns complete ifcJSON-4 object

        Parameters:
        entityAttributes (dict): Dictionary of IFC object data

        Returns:
        dict: containing complete ifcJSON-4 object

        """
        fullObject = {}

        for attr in entityAttributes:

            # Line numbers are not part of IFC JSON
            if attr == 'id':
                continue

            attrKey = self.toLowerCamelcase(attr)

            # Replace wrappedvalue key names to value
            if attrKey == 'wrappedValue':
                attrKey = 'value'

            jsonValue = self.getAttributeValue(entityAttributes[attr])     
            if jsonValue is not None:
                fullObject[attrKey] = jsonValue

        if self.STYLED:
            try:
                # Geometry presentation assignment
                if entityAttributes['type'] == 'IfcStyledItem':
                    # rewrite full Object
                    item = {}
                    item['type'] = fullObject['item']['type']
                    s = str(entityAttributes['Item'])
                    key = s.split("#")[1].split("=")[0]
                    item['ref'] = self.styledMap[key]
                    fullObject['item'] = item
            except Exception as e:
                pass # item is none for a Material presentation assignment
                
        return fullObject

    def createReferenceObject(self, entityAttributes, COMPACT=False):
        """Returns object reference

        Parameters:
        entityAttributes (dict): Dictionary of IFC object data
        COMPACT (boolean): verbose or non verbose ifcJSON-4 output

        Returns:
        dict: object containing reference to another object

        """
        ref = {}
        if not COMPACT:
            ref['type'] = entityAttributes['type']
        ref['ref'] = entityAttributes['GlobalId']
        return ref

    def tessellate(self):
        """Converts all IfcProduct representations to IfcTriangulatedFaceSet
        """
        for product in self.ifcModel.by_type('IfcProduct'):
            if product.Representation:
                try:
                    representation = product.Representation
                    old_shapes = representation.Representations
                    context = old_shapes[0].ContextOfItems

                    tessellated_shape = ifcopenshell.geom.create_shape(
                        self.settings, product)

                    verts = tessellated_shape.geometry.verts
                    vertsList = [verts[i:i+3] for i in range(0, len(verts), 3)]

                    faces = tessellated_shape.geometry.faces
                    facesList = [faces[i:i+3] for i in range(0, len(faces), 3)]

                    pointlist = self.ifcModel.createIfcCartesianPointList3D(
                        vertsList)
                    shape = self.ifcModel.createIfcTriangulatedFaceSet(pointlist,
                        None, None, facesList, None)

                    body_representation = self.ifcModel.createIfcShapeRepresentation(
                        context, "Body", "Tessellation", [shape])
                    new_representation = self.ifcModel.createIfcProductDefinitionShape(
                        None, None, [body_representation])

                    representation = tuple(new_representation)
                    
                    product.Representation = representation

                except Exception as e:
                    print(str(e) + ': Unable to generate OBJ data for ' +
                          str(product))

    def remove_ownerhistory(self):
        for entity in self.ifcModel.by_type('IfcOwnerHistory'):
            self.ifcModel.remove(entity)

    def remove_geometry(self):
        removeTypes = ['IfcLocalPlacement', 'IfcRepresentationMap', 'IfcGeometricRepresentationContext', 'IfcGeometricRepresentationSubContext', 'IfcProductDefinitionShape',
                       'IfcMaterialDefinitionRepresentation', 'IfcShapeRepresentation', 'IfcRepresentationItem', 'IfcStyledRepresentation', 'IfcPresentationLayerAssignment', 'IfcTopologyRepresentation']
        for ifcType in removeTypes:
            # print(ifcType)
            # (lambda x: self.ifcModel.remove(x), self.ifcModel.by_type(ifcType))
            for entity in self.ifcModel.by_type(ifcType):
                self.ifcModel.remove(entity)
