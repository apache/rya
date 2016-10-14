package org.apache.rya.indexing.pcj.matching;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;

import com.google.common.base.Preconditions;

/**
 * Given an order List view of an {@link OptionalJoinSegment} taken
 * from a query and an OptionalJoinSegment representing a PCJ,
 * this class attempts to consolidate the {@link QueryModelNode}s
 * of the PCJ together within the query and order them in a way
 * that is consistent with the PCJ.  This is the key step in matching
 * the PCJ to a subset of a query when LeftJoins are present.
 *
 */
public class PCJNodeConsolidator {

	private TreeSet<PositionNode> leftJoinPosSet = new TreeSet<>(
			new PositionComparator());
	private TreeSet<PositionNode> pcjPosSet = new TreeSet<>(
			new PositionComparator());
	private TreeSet<PositionNode> lowerBoundSet = new TreeSet<>(
			new PositionComparator()); // nonPcjNodes in query that
												// pcjNodes cannot move past
	private TreeSet<PositionNode> upperBoundSet = new TreeSet<>(
			new PositionComparator());// nonPcjNodes in query that pcjNodes
										// cannot move past
	private int greatestLowerBound = -1;
	private int leastUpperBound = Integer.MAX_VALUE;

	private List<QueryModelNode> queryNodes;
	private List<QueryModelNode> pcjNodes;
	private boolean consolidateCalled = false;
	private boolean returnValConsolidate = false;

	public PCJNodeConsolidator(List<QueryModelNode> queryNodes,
			List<QueryModelNode> pcjNodes) {
		Preconditions.checkArgument(new HashSet<QueryModelNode>(queryNodes).containsAll(new HashSet<QueryModelNode>(pcjNodes)));
		this.queryNodes = new ArrayList<>(queryNodes);
		this.pcjNodes = new ArrayList<>(pcjNodes);
		int i = 0;
		for (QueryModelNode q : queryNodes) {
			if (q instanceof FlattenedOptional) {
				leftJoinPosSet.add(new PositionNode(q, i));
			}
			if (pcjNodes.contains(q)) {
				pcjPosSet.add(new PositionNode(q, i));
			}
			i++;
		}
	}

	/**
	 * This method consolidates the PCJ nodes within the query.  After this method is
	 * called, the PCJ nodes in the query will be completely consolidated if true is returned
	 * and will only be partially consolidated if false is return
	 * @return - true or false depending on whether nodes could be entirely consolidated
	 */
	public boolean consolidateNodes() {
		if(consolidateCalled) {
			return returnValConsolidate;
		}
		consolidateCalled = true;
		returnValConsolidate = consolidate() && reOrderPcjNodes();
		return returnValConsolidate;
	}

	/**
	 *
	 * @return List of the query's QueryModelNodes
	 */
	public List<QueryModelNode> getQueryNodes() {
		return queryNodes;
	}

	//assumes nodes are consolidated -- checks if they can be reordered to match pcj node list
	private boolean reOrderPcjNodes() {
		int pos = pcjPosSet.last().getPosition();
		for(int j = pcjNodes.size() - 1; j >= 0; j--) {
			QueryModelNode node = pcjNodes.get(j);
			int i = queryNodes.indexOf(node);
			//use pcj node in queryNodes so FlattenedOptional boundVars
			//are consistent with query
			node = queryNodes.get(i);
			if(!moveQueryNode(new PositionNode(node, i), pos)) {
				return false;
			}
			pos--;
		}
		return true;
	}

	private boolean consolidate() {
		while (canConsolidate()) {
			Move move = getNextMove();
			// if move is empty, then pcj nodes are
			// consolidated
			if (move.isEmpty) {
				return true;
			}
			moveQueryNode(move.node, move.finalPos);
		}

		return false;
	}

	private boolean canConsolidate() {
		if (greatestLowerBound < leastUpperBound) {
			return true;
		}
		return adjustBounds();
	}

	// if l.u.b < g.l.b, attempt to push g.l.b up
	// assume first pcj position node has position less
	// than g.l.b. - this should be by design given that
	// l.u.b <= g.l.b. and there has to be at least one pcj node
	// positioned before l.u.b.
	private boolean adjustBounds() {

		int finalPos = pcjPosSet.first().getPosition();
		PositionNode node = lowerBoundSet.last();

		return moveQueryNode(node, finalPos);
	}

	// assumes g.l.b <= l.u.b.
	// iterates through pcj nodes in query from lowest position to
	// highest looking for a difference in index position greater than
	// one. Given the leftmost pair of nodes separated by two or more
	// spaces, the leftmost node in the pair is moved so that its final
	// position is one position to the left of the rightmost node. For
	// example, given nodes at index 1 and index 3, the node at index 1
	// is advanced to index 2. This method returns the suggested Move,
	// but does not actually perform the Move.
	private Move getNextMove() {

		Iterator<PositionNode> posIterator = pcjPosSet.iterator();
		PositionNode current;
		if (posIterator.hasNext()) {
			current = posIterator.next();
		} else {
			throw new IllegalStateException("PCJ has no nodes!");
		}
		PositionNode next;
		int pos1 = -1;
		int pos2 = -1;
		while (posIterator.hasNext()) {
			next = posIterator.next();
			pos1 = current.getPosition();
			pos2 = next.getPosition();
			// move nodes are not adjacent
			if (pos1 + 1 < pos2) {
				if (leastUpperBound > pos2) {
					return new Move(current, pos2 - 1);
				}
				// pos1 < leastUpperBound < pos2 b/c pos1 < leastUpperBound by
				// design
				else if(greatestLowerBound < pos1) {
					return new Move(next, pos1 + 1);
				}
				//move current to node after greatestLowerBound
				else {
					return new Move(current, greatestLowerBound);
				}
			}

			current = next;
		}

		return new Move();
	}

	private boolean moveQueryNode(PositionNode node, int position) {

		if (!canMoveNode(node, position)) {
			if(upperBoundSet.size() > 0) {
				leastUpperBound = upperBoundSet.first().getPosition();
			}
			if(lowerBoundSet.size() > 0) {
				greatestLowerBound = lowerBoundSet.last().getPosition();
			}
			return false;
		}
		//update QueryModelNodes in leftJoin index so that FlattenedOptional
		//var counts are correct
		updateLeftJoinNodes(node, position);
		//move node in queryNode list
		updateNodeList(node, position, queryNodes);
		//update bounds
		updatePositionNodeSet(node, position, lowerBoundSet);
		updatePositionNodeSet(node, position, upperBoundSet);
		//update leastUppperBound and greatestLowerBound
		if(upperBoundSet.size() > 0) {
			leastUpperBound = upperBoundSet.first().getPosition();
		}
		if(lowerBoundSet.size() > 0) {
			greatestLowerBound = lowerBoundSet.last().getPosition();
		}
		//update positions within leftJoin index
		updatePositionNodeSet(node, position, leftJoinPosSet);
		//no need to update entire set because pcj nodes are not  moved
		//past one another during consolidation
		updatePositionNode(node, position, pcjPosSet);

		return true;
	}

	private boolean canMoveNode(PositionNode node, int finalPos) {
		PositionNode bound = getBounds(node, finalPos, queryNodes, leftJoinPosSet);
		if (bound.isEmpty) {
			return true;
		}
		addBound(bound, node, finalPos);
		return false;

	}

	//adds bound to either lowerBoundSet or uppderBoundSet, depending on initial and
	//final position of move
	private void addBound(PositionNode bound, PositionNode node, int finalPos) {
		int diff = finalPos - node.getPosition();

		if(diff == 0) {
			return;
		}

		if (diff > 0) {
			if (upperBoundSet.contains(bound)) {
				return;
			} else {
				upperBoundSet.add(bound);
			}
		} else {
			if (lowerBoundSet.contains(bound)) {
				return;
			} else {
				lowerBoundSet.add(bound);
			}
		}
	}


	// updates nodes in given TreeSet between node.getPosition() and position
	private void updatePositionNodeSet(PositionNode node, int position,
			TreeSet<PositionNode> set) {

		if(set.size() == 0) {
			return;
		}

		int oldPos = node.getPosition();
		int diff = position - oldPos;
		SortedSet<PositionNode> posNodes;
		boolean containsNode = false;

		if (diff == 0) {
			return;
		}

		//remove node before decrementing or incrementing to prevent overwriting
		if(set.contains(node)) {
			containsNode = true;
			set.remove(node);
		}

		if (diff > 0) {
			posNodes = set
					.subSet(node, false, new PositionNode(position), true);

			List<PositionNode> pNodeList = new ArrayList<>();
			for(PositionNode pos: posNodes) {
				pNodeList.add(pos);
			}
			// decrement posNodes
			for (PositionNode pos : pNodeList) {
				int p = pos.getPosition() - 1;
				updatePositionNode(pos, p, set);
			}
		} else {
			posNodes = set
					.subSet(new PositionNode(position), true, node, false);
			//create list to iterate in reverse order
			List<PositionNode> pNodeList = new ArrayList<>();
			for(PositionNode pos: posNodes) {
				pNodeList.add(pos);
			}
			//increment elements of TreeSet in reverse order so
			//that no collisions occur - PositionNodes are incremented
			//into slot created by removing node
			for(int i = pNodeList.size() - 1; i >= 0; i--) {
				PositionNode pNode = pNodeList.get(i);
				int p = pNode.getPosition() + 1;
				updatePositionNode(pNode, p, set);
			}
		}

		if(containsNode) {
			node.setPosition(position);
			set.add(node);
		}

	}

	//updates the var counts in specified left join index
	private void updateLeftJoinNodes(PositionNode node, int finalPos) {
		if(node.getNode() instanceof ValueExpr) {
			return;
		}

		int diff = finalPos - node.getPosition();

		if (diff == 0) {
			return;
		}

		if (node.isOptional) {
			leftJoinPosSet.remove(node);
			FlattenedOptional optional = (FlattenedOptional)node.getNode();
			if (diff < 0) {
				for (int i = node.getPosition() - 1; i > finalPos - 1; i--) {
					QueryModelNode tempNode = queryNodes.get(i);
					if (tempNode instanceof ValueExpr) {
						continue;
					}
					optional.addArg((TupleExpr) tempNode);
				}
			} else {
				for (int i = node.getPosition() + 1; i < finalPos + 1; i++) {
					QueryModelNode tempNode = queryNodes.get(i);
					if (tempNode instanceof ValueExpr) {
						continue;
					}
					optional.removeArg((TupleExpr) tempNode);
				}
			}
			node.setNode(optional);
			//FlattenedOptional equals does not take into account var counts
			//The following three lines update the var count in the optional in list
			int index = queryNodes.indexOf(optional);
			queryNodes.remove(optional);
			queryNodes.add(index, optional);
			leftJoinPosSet.add(node);

		} else {
			TupleExpr te = (TupleExpr) node.getNode();
			SortedSet<PositionNode> optionals;
			if (diff < 0) {
				optionals = leftJoinPosSet.subSet(new PositionNode(finalPos), true, node, false);
				for (PositionNode pNode : optionals) {
					FlattenedOptional optional = (FlattenedOptional) pNode
							.getNode();
					optional.removeArg(te);
				}
			} else {
				optionals = leftJoinPosSet.subSet(node, false, new PositionNode(finalPos), true);
				for (PositionNode pNode : optionals) {
					FlattenedOptional optional = (FlattenedOptional) pNode
							.getNode();
					optional.addArg(te);
				}
			}
		}

	}



	//works only if moving node to final position does not move it across
	//another node in set
	private void updatePositionNode(PositionNode node, int position,
			TreeSet<PositionNode> set) {
		set.remove(node);
		node.setPosition(position);
		set.add(node);
	}

	// assumes input data fall within capacity of list
	private void updateNodeList(PositionNode node, int finalPos,
			List<QueryModelNode> list) {
		int initialPos = node.getPosition();
		QueryModelNode qNode = list.remove(initialPos);
		if (finalPos < list.size()) {
			list.add(finalPos, qNode);
		} else {
			list.add(qNode);
		}
	}

	/**
	 *
	 * @param node
	 * @param finalPos
	 * @param list
	 * @param leftJoinNodes
	 * @return PositionNode - if node cannot be move to final position, this
	 * method returns a non-empty PositionNode representing a bound to the move.
	 * If it can, it returns an empty PositionNode.
	 */
	// determine if given node can be moved to finalPos
	// assumes node.position and finalPos fall within index range of list
	private PositionNode getBounds(PositionNode node, int finalPos,
			List<QueryModelNode> list, TreeSet<PositionNode> leftJoinNodes) {

		//filters can be moved up and pushed down join segment
		//without affecting bound and unbound variables of
		//FlattenedOptionals -- Filters can be pushed down as
		//far as possible because it is assumed that any variable
		//that appears in a Filter also appears in a PCJ node
		//if Filters can be grouped, then Filter variables will
		//automatically be bound
		if(node.getNode() instanceof ValueExpr) {
			return new PositionNode();
		}

		int diff = finalPos - node.getPosition();

		if (diff == 0) {
			return new PositionNode();
		}

		if (node.isOptional) {
			FlattenedOptional optional = ((FlattenedOptional)node.getNode()).clone();
			if (diff < 0) {
				for (int i = node.getPosition() - 1; i > finalPos - 1; i--) {
					QueryModelNode tempNode = list.get(i);
					if (tempNode instanceof ValueExpr) {
						continue;
					}

					if (!optional.canAddTuple((TupleExpr) tempNode)) {
						return new PositionNode(tempNode, i);
					}

					if(tempNode instanceof FlattenedOptional) {
						FlattenedOptional tempOptional = (FlattenedOptional) tempNode;
						if(!tempOptional.canRemoveTuple(optional)) {
							return new PositionNode(tempNode, i);
						}

					}
					optional.addArg((TupleExpr) tempNode);
				}
			} else {
				for (int i = node.getPosition() + 1; i < finalPos + 1; i++) { // TODO
																				// check
																				// bounds
					QueryModelNode tempNode = list.get(i);
					if (tempNode instanceof ValueExpr) {
						continue;
					}
					if (!optional.canRemoveTuple((TupleExpr) tempNode)) {
						return new PositionNode(tempNode, i);
					}

					if(tempNode instanceof FlattenedOptional) {
						FlattenedOptional tempOptional = (FlattenedOptional) tempNode;
						if(!tempOptional.canAddTuple(optional)) {
							return new PositionNode(tempNode, i);
						}
					}
					optional.removeArg((TupleExpr) tempNode);
				}
			}

			return new PositionNode();

		} else {
			TupleExpr te = (TupleExpr) node.getNode();
			SortedSet<PositionNode> leftJoins;
			if (diff < 0) {
				leftJoins = leftJoinNodes.subSet(new PositionNode(finalPos), true, node, false);

				for (PositionNode pNode : leftJoins) {
					FlattenedOptional optional = (FlattenedOptional) pNode
							.getNode();
					if (!optional.canRemoveTuple(te)) {
						return new PositionNode(pNode);
					}
				}
			} else {

				leftJoins = leftJoinNodes.subSet(node, false, new PositionNode(finalPos), true);
				for (PositionNode pNode : leftJoins) {
					FlattenedOptional optional = (FlattenedOptional) pNode
							.getNode();
					if (!optional.canAddTuple(te)) {
						return new PositionNode(pNode);
					}
				}
			}

			return new PositionNode();

		}

	}


	static class Move {

		PositionNode node;
		int finalPos;
		boolean isEmpty = true;

		public Move(PositionNode node, int finalPos) {
			this.node = node;
			this.finalPos = finalPos;
			this.isEmpty = false;
		}

		public Move() {
		}

	}

	static class PositionNode {

		private int position;
		private QueryModelNode node;
		private boolean isOptional = false;
		boolean isEmpty = true;

		public PositionNode(QueryModelNode node, int position) {
			this.node = node;
			this.position = position;
			this.isOptional = node instanceof FlattenedOptional;
			isEmpty = false;
		}

		public PositionNode(PositionNode node) {
			this(node.node, node.position);
		}

		public PositionNode(int position) {
			this.position = position;
			isEmpty = false;
		}

		public PositionNode() {

		}

		/**
		 * @return the position
		 */
		public int getPosition() {
			return position;
		}

		/**
		 * @param position
		 *            the position to set
		 */
		public void setPosition(int position) {
			this.position = position;
		}

		/**
		 * @return the node
		 */
		public QueryModelNode getNode() {
			return node;
		}

		public void setNode(QueryModelNode node) {
			this.node = node;
		}

		public boolean isOptional() {
			return isOptional;
		}

		@Override
		public String toString() {
			return "Node: " + node + " Position: " + position;
		}

	}


	class PositionComparator implements Comparator<PositionNode> {

		@Override
		public int compare(PositionNode node1, PositionNode node2) {

			if (node1.position < node2.position) {
				return -1;
			}
			if (node1.position > node2.position) {
				return 1;
			}

			return 0;
		}

	}

}
