use proc_macro::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    parse_macro_input, Expr, ExprLit, FnArg, GenericArgument, ItemFn, Lit, Meta, Pat,
    PathArguments, ReturnType, Token, Type,
};

struct MacroArgs {
    items: Punctuated<Meta, Token![,]>,
}

impl Parse for MacroArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self {
            items: Punctuated::parse_terminated(input)?,
        })
    }
}

#[proc_macro_attribute]
pub fn workflow(args: TokenStream, item: TokenStream) -> TokenStream {
    expand_handler(args, item, HandlerKind::Workflow)
}

#[proc_macro_attribute]
pub fn activity(args: TokenStream, item: TokenStream) -> TokenStream {
    expand_handler(args, item, HandlerKind::Activity)
}

enum HandlerKind {
    Workflow,
    Activity,
}

fn expand_handler(args: TokenStream, item: TokenStream, kind: HandlerKind) -> TokenStream {
    let args = parse_macro_input!(args as MacroArgs);
    let item_fn = parse_macro_input!(item as ItemFn);

    match expand_handler_inner(args, item_fn, kind) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_handler_inner(
    args: MacroArgs,
    item_fn: ItemFn,
    kind: HandlerKind,
) -> syn::Result<proc_macro2::TokenStream> {
    if item_fn.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.fn_token,
            "durust handlers must be async functions",
        ));
    }

    let parsed = ParsedArgs::from(args)?;
    if matches!(kind, HandlerKind::Workflow) {
        lint_workflow_body(&item_fn)?;
    }

    let vis = &item_fn.vis;
    let attrs = &item_fn.attrs;
    let ident = &item_fn.sig.ident;
    let impl_ident = format_ident!("__durust_impl_{}", ident);
    let (input_binding, input) = extract_single_input(&item_fn)?;
    let output = extract_result_output(&item_fn)?;
    let name = parsed.name.unwrap_or_else(|| ident.to_string());
    let rust_path = quote!(concat!(env!("CARGO_PKG_NAME"), "::", module_path!(), "::", stringify!(#ident)));
    let block = &item_fn.block;

    let version_impl = match kind {
        HandlerKind::Workflow => {
            let version = parsed.version.ok_or_else(|| {
                syn::Error::new_spanned(
                    &item_fn.sig.ident,
                    "#[durust::workflow] requires `version = <u32>`",
                )
            })?;
            quote! {
                const VERSION: u32 = #version;
            }
        }
        HandlerKind::Activity => quote! {},
    };

    let trait_name = match kind {
        HandlerKind::Workflow => quote!(::durust::Workflow),
        HandlerKind::Activity => quote!(::durust::Activity),
    };

    let run_fn = match kind {
        HandlerKind::Workflow => quote! {
            fn run(self, input: Self::Input) -> ::durust::BoxWorkflowFuture<Self::Output> {
                Box::pin(async move { #impl_ident(input).await })
            }
        },
        HandlerKind::Activity => quote! {
            fn run(self, input: Self::Input) -> ::durust::BoxActivityFuture<Self::Output> {
                Box::pin(async move { #impl_ident(input).await })
            }
        },
    };

    Ok(quote! {
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug, Default)]
        #vis struct #ident;

        impl #trait_name for #ident {
            type Input = #input;
            type Output = #output;

            const NAME: &'static str = #name;
            #version_impl
            const RUST_PATH: &'static str = #rust_path;

            #run_fn
        }

        #(#attrs)*
        async fn #impl_ident(input: #input) -> ::durust::Result<#output> {
            let #input_binding = input;
            #block
        }
    })
}

fn extract_single_input(item_fn: &ItemFn) -> syn::Result<(Pat, Type)> {
    if item_fn.sig.inputs.len() != 1 {
        return Err(syn::Error::new_spanned(
            &item_fn.sig.inputs,
            "durust handlers must take exactly one input argument",
        ));
    }

    match item_fn.sig.inputs.first().expect("checked input count") {
        FnArg::Typed(arg) => Ok(((*arg.pat).clone(), (*arg.ty).clone())),
        FnArg::Receiver(receiver) => Err(syn::Error::new_spanned(
            receiver,
            "durust handlers cannot take self",
        )),
    }
}

fn extract_result_output(item_fn: &ItemFn) -> syn::Result<Type> {
    let ReturnType::Type(_, ty) = &item_fn.sig.output else {
        return Err(syn::Error::new_spanned(
            &item_fn.sig,
            "durust handlers must return durust::Result<T>",
        ));
    };

    let Type::Path(type_path) = ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            ty,
            "durust handlers must return durust::Result<T>",
        ));
    };

    let Some(segment) = type_path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            ty,
            "durust handlers must return durust::Result<T>",
        ));
    };

    if segment.ident != "Result" {
        return Err(syn::Error::new_spanned(
            ty,
            "durust handlers must return durust::Result<T>",
        ));
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(syn::Error::new_spanned(
            ty,
            "durust handlers must return durust::Result<T>",
        ));
    };

    let Some(GenericArgument::Type(output)) = args.args.first() else {
        return Err(syn::Error::new_spanned(
            ty,
            "durust handlers must return durust::Result<T>",
        ));
    };

    Ok(output.clone())
}

#[derive(Default)]
struct ParsedArgs {
    name: Option<String>,
    version: Option<u32>,
}

impl ParsedArgs {
    fn from(args: MacroArgs) -> syn::Result<Self> {
        let mut parsed = Self::default();
        for meta in args.items {
            match meta {
                Meta::Path(path) if path.is_ident("strict") => {}
                Meta::NameValue(name_value) if name_value.path.is_ident("name") => {
                    parsed.name = Some(lit_string(&name_value.value, "name")?);
                }
                Meta::NameValue(name_value) if name_value.path.is_ident("version") => {
                    parsed.version = Some(lit_u32(&name_value.value, "version")?);
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "unsupported durust handler attribute argument",
                    ));
                }
            }
        }
        Ok(parsed)
    }
}

fn lit_string(expr: &Expr, field: &str) -> syn::Result<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Ok(value.value()),
        _ => Err(syn::Error::new_spanned(
            expr,
            format!("`{field}` must be a string literal"),
        )),
    }
}

fn lit_u32(expr: &Expr, field: &str) -> syn::Result<u32> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Int(value),
            ..
        }) => value.base10_parse(),
        _ => Err(syn::Error::new_spanned(
            expr,
            format!("`{field}` must be an integer literal"),
        )),
    }
}

fn lint_workflow_body(item_fn: &ItemFn) -> syn::Result<()> {
    let source = item_fn.block.to_token_stream().to_string();
    let forbidden = [
        ("tokio :: time :: sleep", "use durust::sleep instead"),
        ("tokio :: select", "use durust::select! instead"),
        ("tokio :: spawn", "use durust::spawn or durust::join! instead"),
        ("std :: time :: Instant :: now", "use durust::now instead"),
        ("std :: time :: SystemTime :: now", "use durust::now instead"),
        ("rand :: random", "use durust::side_effect instead"),
    ];

    for (needle, suggestion) in forbidden {
        if source.contains(needle) {
            return Err(syn::Error::new_spanned(
                &item_fn.sig.ident,
                format!("nondeterministic workflow API detected; {suggestion}"),
            ));
        }
    }

    Ok(())
}
