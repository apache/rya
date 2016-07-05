<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE rdf:RDF [
   <!ENTITY xsd  "http://www.w3.org/2001/XMLSchema#" >
 ]>

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	xmlns:sparql="http://www.w3.org/2005/sparql-results#" xmlns="http://www.w3.org/1999/xhtml">

	<xsl:include href="../locale/messages.xsl" />

	<xsl:variable name="title">
		<xsl:value-of select="$repository-create.title" />
	</xsl:variable>

	<xsl:include href="template.xsl" />

	<xsl:template match="sparql:sparql">
		<script src="../../scripts/create.js" type="text/javascript">
		</script>
		<form action="create" method="post">
			<table class="dataentry">
				<tbody>
					<tr>
						<th>
							<xsl:value-of select="$repository-type.label" />
						</th>
						<td>
							<select id="type" name="type">
								<option value="RyaAccumuloSail">
									Rya Accumulo Store
								</option>
							</select>
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							<xsl:value-of select="$repository-id.label" />
						</th>
						<td>
							<input type="text" id="id" name="Repository ID" size="16"
								value="RyaAccumuloSail" />
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							<xsl:value-of select="$repository-title.label" />
						</th>
						<td>
							<input type="text" id="title" name="Repository title" size="48"
								value="RyaAccumuloSail Store" />
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							Accumulo User
						</th>
						<td>
							<input type="text" id="user" name="Rya Accumulo user" size="32"
								value="root" />
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							Accumulo Password
						</th>
						<td>
							<input type="text" id="password" name="Rya Accumulo password" size="32" value="root" />
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							Accumulo Instance
						</th>
						<td>
							<input type="text" id="instance" name="Rya Accumulo instance" size="32" value="dev" />
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							Zookeepers
						</th>
						<td>
							<input type="text" id="zoo" name="Rya Accumulo zookeepers" size="32"
								value="localhost" />
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							is Mock?
						</th>
						<td>
							<input type="text" id="ismock" name="Rya Accumulo is mock"
								size="32" value="false"/>
						</td>
						<td></td>
					</tr>
					<tr>
						<td></td>
						<td>
							<input type="button" value="{$cancel.label}" style="float:right"
								href="repositories" onclick="document.location.href=this.getAttribute('href')" />
							<input id="create" type="button" value="{$create.label}"
								onclick="checkOverwrite()" />
						</td>
					</tr>
				</tbody>
			</table>
		</form>
	</xsl:template>

</xsl:stylesheet>
