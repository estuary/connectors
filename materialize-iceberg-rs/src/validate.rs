use anyhow::Result;
use proto_flow::flow::Projection;
use proto_flow::flow::SerPolicy;
use proto_flow::materialize::request::Validate;
use proto_flow::materialize::response::validated::Binding;
use proto_flow::materialize::response::validated::Constraint;
use proto_flow::materialize::response::validated::ProjectionConstraint;
use proto_flow::materialize::response::validated::constraint;

use crate::configuration::Resource;

pub async fn do_validate(req: Validate) -> Result<Vec<Binding>> {
    req.bindings
        .iter()
        .map(|binding| {
            let res: Resource = serde_json::from_slice(&binding.resource_config_json)?;

            Ok(Binding {
                projection_constraints: binding
                    .collection
                    .as_ref()
                    .expect("binding must have collection spec")
                    .projections
                    .iter()
                    .map(|p| ProjectionConstraint {
                        field: p.field.clone(),
                        constraint: Some(constraint_for_projection(p)),
                    })
                    .collect(),
                resource_path: vec![res.table],
                delta_updates: true,
                case_insensitive_fields: false,
                ser_policy: Some(SerPolicy {
                    str_truncate_after: 1 << 16,
                    nested_obj_truncate_after: 1000,
                    array_truncate_after: 1000,
                }),
            })
        })
        .collect::<Result<Vec<Binding>>>()
}

fn constraint_for_projection(p: &Projection) -> Constraint {
    if p.is_primary_key {
        Constraint {
            r#type: constraint::Type::LocationRecommended.into(),
            reason: "Primary key locations should usually be materialized".to_string(),
            ..Default::default()
        }
    } else if p.ptr.is_empty() {
        // The connector stores the root document verbatim alongside the key
        // columns and has no other representation of a row's data, so this
        // location must always be selected.
        Constraint {
            r#type: constraint::Type::LocationRequired.into(),
            reason: "The root document must be materialized".to_string(),
            ..Default::default()
        }
    } else {
        Constraint {
            r#type: constraint::Type::LocationRecommended.into(),
            reason: "This location should usually be materialized".to_string(),
            ..Default::default()
        }
    }
}
